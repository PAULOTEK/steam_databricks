# --- Multiple Kafka Topics to Bronze Tables ---
# Este notebook mostra como ler de múltiplos tópicos Kafka simultaneamente
# e criar tabelas Bronze separadas para cada tópico

# --- Parameters ---
confluentBootstrapServers = "pkc-*******.eastus.azure.confluent.cloud:9092"
confluentApiKey = "*****************"
confluentSecret = "****************************************"

# Configuração de múltiplos tópicos
kafka_topics_config = {
    "sample_data_users": {
        "schema": StructType([
            StructField("registertime", LongType(), True),
            StructField("userid", StringType(), True),
            StructField("regionid", StringType(), True),
            StructField("gender", StringType(), True)
        ]),
        "table_name": "bronze.users_kafka",
        "checkpoint": "s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/users_kafka"
    },
    "sample_data_orders": {
        "schema": StructType([
            StructField("orderid", StringType(), True),
            StructField("userid", StringType(), True),
            StructField("productid", StringType(), True),
            StructField("quantity", LongType(), True),
            StructField("price", LongType(), True),
            StructField("orderdate", LongType(), True)
        ]),
        "table_name": "bronze.orders_kafka",
        "checkpoint": "s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/orders_kafka"
    },
    "sample_data_products": {
        "schema": StructType([
            StructField("productid", StringType(), True),
            StructField("productname", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", LongType(), True)
        ]),
        "table_name": "bronze.products_kafka",
        "checkpoint": "s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/products_kafka"
    }
}

from pyspark.sql.functions import col, from_json, expr, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType

# --- Função para processar um tópico Kafka ---
def process_kafka_topic(topic_name, config):
    """
    Processa um único tópico Kafka e escreve para uma tabela Bronze.
    """
    
    # Configuração do Kafka
    kafka_options = {
        "kafka.bootstrap.servers": confluentBootstrapServers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluentApiKey}" password="{confluentSecret}";',
        "subscribe": topic_name,
        "startingOffsets": "latest",
    }
    
    # Read Stream from Kafka
    df_raw = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )
    
    # Clean and parse JSON
    df_clean = df_raw.withColumn(
        "clean_value",
        expr("regexp_replace(CAST(value AS STRING), '^[^\\{]*', '')")
    )
    
    df_parsed = df_clean.select(
        from_json(col("clean_value"), config["schema"]).alias("data")
    ).select("data.*")
    
    # Adiciona metadados
    df_bronze = df_parsed.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    ).withColumn(
        "source",
        lit("kafka")
    ).withColumn(
        "topic",
        lit(topic_name)
    )
    
    # Criar tabela Bronze se não existir
    table_name = config["table_name"]
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {','.join([f"{field.name} {field.dataType.typeName()}" for field in config["schema"].fields])},
        ingestion_timestamp TIMESTAMP,
        source STRING,
        topic STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    # Write Stream to Bronze Table
    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["checkpoint"])
        .option("mergeSchema", "true")
        .toTable(table_name)
    )
    
    return query

# --- Processar todos os tópicos ---
queries = []

for topic_name, config in kafka_topics_config.items():
    print(f"Starting stream for topic: {topic_name}")
    try:
        query = process_kafka_topic(topic_name, config)
        queries.append(query)
        print(f"Stream started successfully for topic: {topic_name}")
    except Exception as e:
        print(f"Error processing topic {topic_name}: {str(e)}")

# --- Monitoramento dos Streams ---
print(f"\nTotal active streams: {len(queries)}")

for i, query in enumerate(queries):
    print(f"\nStream {i+1} status:")
    print(f"Status: {query.status}")
    print(f"Last Progress: {query.lastProgress}")

# --- Consultar as tabelas Bronze ---
# Tabela de usuários
spark.read.table("bronze.users_kafka").display()

# Tabela de pedidos
spark.read.table("bronze.orders_kafka").display()

# Tabela de produtos
spark.read.table("bronze.products_kafka").display()

# --- Consultas cruzadas entre tabelas ---
# Join entre usuários e pedidos
spark.sql("""
SELECT 
    u.userid,
    u.regionid,
    u.gender,
    o.orderid,
    o.productid,
    o.quantity,
    o.price
FROM bronze.users_kafka u
INNER JOIN bronze.orders_kafka o ON u.userid = o.userid
LIMIT 10
""").display()

# Join entre pedidos e produtos
spark.sql("""
SELECT 
    o.orderid,
    o.userid,
    p.productname,
    p.category,
    o.quantity,
    o.price,
    o.orderdate
FROM bronze.orders_kafka o
INNER JOIN bronze.products_kafka p ON o.productid = p.productid
LIMIT 10
""").display()

# --- Análises agregadas ---
# Vendas por categoria
spark.sql("""
SELECT 
    p.category,
    COUNT(*) as total_orders,
    SUM(o.quantity) as total_quantity,
    SUM(o.price * o.quantity) as total_revenue
FROM bronze.orders_kafka o
INNER JOIN bronze.products_kafka p ON o.productid = p.productid
GROUP BY p.category
ORDER BY total_revenue DESC
""").display()

# Usuários por região
spark.sql("""
SELECT 
    regionid,
    COUNT(DISTINCT userid) as unique_users,
    COUNT(*) as total_records
FROM bronze.users_kafka
GROUP BY regionid
ORDER BY unique_users DESC
""").display()

# --- Alternativa: Ler todos os tópicos em um único stream ---
# Se preferir ler todos os tópicos em um único stream e separar depois:

kafka_options_all = {
    "kafka.bootstrap.servers": confluentBootstrapServers,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluentApiKey}" password="{confluentSecret}";',
    "subscribe": ",".join(kafka_topics_config.keys()),  # Todos os tópicos
    "startingOffsets": "latest",
}

df_all_topics = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options_all)
    .load()
)

# O campo 'topic' contém o nome do tópico de origem
df_with_topic = df_all_topics.withColumn("topic_name", col("topic"))

# Você pode usar quando/otherwise para processar diferentes esquemas por tópico
# from pyspark.sql.functions import when
# df_processed = df_with_topic.withColumn(
#     "parsed_data",
#     when(col("topic_name") == "sample_data_users", from_json(...))
#     .when(col("topic_name") == "sample_data_orders", from_json(...))
#     ...
# )

# --- Limpeza ---
# Para parar todos os streams:
# for query in queries:
#     query.stop()

# Para remover tabelas:
# spark.sql("DROP TABLE IF EXISTS bronze.users_kafka")
# spark.sql("DROP TABLE IF EXISTS bronze.orders_kafka")
# spark.sql("DROP TABLE IF EXISTS bronze.products_kafka")

# Para remover checkpoints:
# dbutils.fs.rm("s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/users_kafka", recurse=True)
# dbutils.fs.rm("s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/orders_kafka", recurse=True)
# dbutils.fs.rm("s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/products_kafka", recurse=True)
