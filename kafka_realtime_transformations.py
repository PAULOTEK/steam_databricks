# --- Kafka Real-time Transformations ---
# Este notebook mostra como fazer transformações em tempo real nos dados do Kafka
# antes de escrever para a tabela Bronze

# --- Parameters ---
confluentBootstrapServers = "pkc-*******.eastus.azure.confluent.cloud:9092"
confluentTopicName = "sample_data_users"
confluentApiKey = "*****************"
confluentSecret = "****************************************"

from pyspark.sql.functions import col, from_json, expr, current_timestamp, lit, when, upper, trim, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.window import Window

# Schema dos dados
schema = StructType([
    StructField("registertime", LongType(), True),
    StructField("userid", StringType(), True),
    StructField("regionid", StringType(), True),
    StructField("gender", StringType(), True)
])

# --- Configuração do Kafka ---
kafka_options = {
    "kafka.bootstrap.servers": confluentBootstrapServers,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluentApiKey}" password="{confluentSecret}";',
    "subscribe": confluentTopicName,
    "startingOffsets": "latest",
}

# --- Read Stream from Kafka ---
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# --- Transformações em Tempo Real ---

# 1. Limpeza e Parse
df_clean = df_raw.withColumn(
    "clean_value",
    expr("regexp_replace(CAST(value AS STRING), '^[^\\{]*', '')")
)

df_parsed = df_clean.select(
    from_json(col("clean_value"), schema).alias("data")
).select("data.*")

# 2. Validação e Limpeza de Dados
df_validated = df_parsed.filter(
    col("userid").isNotNull() &
    col("regionid").isNotNull()
).withColumn(
    "userid_clean",
    trim(col("userid"))
).withColumn(
    "regionid_clean",
    upper(trim(col("regionid")))
).withColumn(
    "gender_clean",
    when(col("gender").isNotNull(), upper(trim(col("gender"))))
    .otherwise("UNKNOWN")
)

# 3. Enriquecimento de Dados
df_enriched = df_validated.withColumn(
    "registertime_dt",
    expr("from_unixtime(registertime)")
).withColumn(
    "registration_date",
    expr("to_date(from_unixtime(registertime))")
).withColumn(
    "registration_year",
    expr("year(from_unixtime(registertime))")
).withColumn(
    "registration_month",
    expr("month(from_unixtime(registertime))")
).withColumn(
    "region_category",
    when(col("regionid_clean").isin("US-WEST", "US-EAST", "US-CENTRAL"), "US")
    .when(col("regionid_clean").isin("EU-WEST", "EU-EAST", "EU-CENTRAL"), "EU")
    .when(col("regionid_clean").isin("ASIA-PACIFIC", "ASIA-EAST", "ASIA-SOUTH"), "ASIA")
    .otherwise("OTHER")
)

# 4. Adicionar Metadados
df_bronze = df_enriched.withColumn(
    "ingestion_timestamp",
    current_timestamp()
).withColumn(
    "ingestion_date",
    current_date()
).withColumn(
    "source",
    lit("kafka")
).withColumn(
    "topic",
    lit(confluentTopicName)
).withColumn(
    "processing_latency_ms",
    expr("(unix_timestamp(current_timestamp()) - registertime) * 1000")
)

# 5. Watermarking para windowed operations (opcional)
# Útil para agregações em tempo real
df_with_watermark = df_bronze.withWatermark(
    "ingestion_timestamp",
    "5 minutes"  # Tolerância de 5 minutos para dados tardios
)

# --- Configuração da Tabela Bronze ---
bronze_table_name = "bronze.users_transformed"
checkpoint_location = "s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/users_transformed"

# Criar a tabela Bronze se não existir
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_table_name} (
    registertime LONG,
    userid STRING,
    regionid STRING,
    gender STRING,
    userid_clean STRING,
    regionid_clean STRING,
    gender_clean STRING,
    registertime_dt STRING,
    registration_date DATE,
    registration_year INT,
    registration_month INT,
    region_category STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_date DATE,
    source STRING,
    topic STRING,
    processing_latency_ms LONG
)
USING DELTA
PARTITIONED BY (ingestion_date, region_category)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")

# --- Write Stream to Bronze Table ---
query = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .partitionBy("ingestion_date", "region_category")  # Particionamento para performance
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")  # Processa a cada 30 segundos
    .toTable(bronze_table_name)
)

# --- Agregações em Tempo Real (usando watermark) ---
# Criar uma tabela de métricas em tempo real
metrics_table_name = "bronze.users_realtime_metrics"
metrics_checkpoint = "s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/users_realtime_metrics"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metrics_table_name} (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    region_category STRING,
    gender_clean STRING,
    user_count LONG,
    unique_users LONG,
    avg_latency_ms DOUBLE
)
USING DELTA
""")

# Agregação por janela de tempo
df_metrics = df_with_watermark.groupBy(
    expr("window(ingestion_timestamp, '5 minutes')").alias("time_window"),
    col("region_category"),
    col("gender_clean")
).agg(
    count("*").alias("user_count"),
    countDistinct("userid_clean").alias("unique_users"),
    avg("processing_latency_ms").alias("avg_latency_ms")
).select(
    col("time_window.start").alias("window_start"),
    col("time_window.end").alias("window_end"),
    col("region_category"),
    col("gender_clean"),
    col("user_count"),
    col("unique_users"),
    col("avg_latency_ms")
)

query_metrics = (
    df_metrics.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", metrics_checkpoint)
    .trigger(processingTime="1 minute")
    .toTable(metrics_table_name)
)

# --- Consultas Úteis ---

# Dados brutos transformados
spark.read.table(bronze_table_name).display()

# Métricas em tempo real
spark.read.table(metrics_table_name).display()

# Últimos 10 minutos de dados
spark.sql(f"""
SELECT * FROM {bronze_table_name} 
WHERE ingestion_timestamp >= current_timestamp() - interval 10 minutes
ORDER BY ingestion_timestamp DESC
LIMIT 100
""").display()

# Latência de processamento por região
spark.sql(f"""
SELECT 
    region_category,
    AVG(processing_latency_ms) as avg_latency,
    MAX(processing_latency_ms) as max_latency,
    MIN(processing_latency_ms) as min_latency,
    COUNT(*) as total_records
FROM {bronze_table_name}
GROUP BY region_category
ORDER BY avg_latency DESC
""").display()

# Volume de dados por hora
spark.sql(f"""
SELECT 
    date_trunc('hour', ingestion_timestamp) as hour,
    region_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT userid_clean) as unique_users
FROM {bronze_table_name}
GROUP BY date_trunc('hour', ingestion_timestamp), region_category
ORDER BY hour DESC, record_count DESC
""").display()

# Detecção de anomalias (latência alta)
spark.sql(f"""
SELECT 
    userid_clean,
    regionid_clean,
    processing_latency_ms,
    ingestion_timestamp
FROM {bronze_table_name}
WHERE processing_latency_ms > 60000  -- Mais de 1 minuto de latência
ORDER BY processing_latency_ms DESC
LIMIT 20
""").display()

# --- Batch Processing do histórico ---
# Se você precisar processar dados históricos do Kafka:
# Mude "startingOffsets": "latest" para "startingOffsets": "earliest"
# E use .trigger(once=True) para processar uma vez e parar

# query_batch = (
#     df_bronze.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", checkpoint_location + "_batch")
#     .trigger(once=True)  # Processa uma vez e para
#     .toTable(bronze_table_name + "_batch")
# )

# --- Monitoramento e Alertas ---
# Verificar se o streaming está saudável
print(f"Streaming status: {query.status}")
print(f"Metrics streaming status: {query_metrics.status}")

# Taxa de processamento
print(f"Streaming rate: {query.lastProgress.get('numInputRows', 0)} rows/trigger")

# --- Limpeza ---
# Para parar os streams:
# query.stop()
# query_metrics.stop()

# Para remover tabelas:
# spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name}")
# spark.sql(f"DROP TABLE IF EXISTS {metrics_table_name}")

# Para remover checkpoints:
# dbutils.fs.rm(checkpoint_location, recurse=True)
# dbutils.fs.rm(metrics_checkpoint, recurse=True)
