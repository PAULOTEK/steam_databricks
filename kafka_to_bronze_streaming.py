# --- Kafka to Bronze Table Streaming ---
# Este notebook mostra como ler dados do Kafka em tempo real e alimentar uma tabela Bronze
# O streaming contínuo fica lendo novos eventos e adicionando à tabela

# --- Parameters ---
confluentBootstrapServers = "pkc-*******.eastus.azure.confluent.cloud:9092"
confluentTopicName = "sample_data_users"
confluentApiKey = "*****************"
confluentSecret = "****************************************"

# Schema dos dados do Kafka
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
    "startingOffsets": "latest",  # "latest" para novos eventos, "earliest" para histórico
}

# --- Read Stream from Kafka ---
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# --- Parse JSON Data ---
# Remove caracteres não-JSON do campo value
df_clean = df_raw.withColumn(
    "clean_value",
    expr("regexp_replace(CAST(value AS STRING), '^[^\\{]*', '')")
)

# Parse o JSON limpo
df_parsed = df_clean.select(
    from_json(col("clean_value"), schema).alias("data")
).select("data.*")

# Adiciona colunas de metadados
df_bronze = df_parsed.withColumn(
    "ingestion_timestamp",
    current_timestamp()
).withColumn(
    "source",
    lit("kafka")
).withColumn(
    "topic",
    lit(confluentTopicName)
)

# --- Configuração da Tabela Bronze ---
bronze_table_name = "bronze.users_kafka_streaming"
checkpoint_location = "s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/bronze_users_streaming"

# Criar a tabela Bronze se não existir
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_table_name} (
    registertime LONG,
    userid STRING,
    regionid STRING,
    gender STRING,
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

# --- Write Stream to Bronze Table ---
# O streaming vai ficar rodando continuamente, adicionando novos dados à tabela
query = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")  # Adiciona novos registros à tabela
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .toTable(bronze_table_name)
)

# O streaming vai ficar rodando e processando novos eventos automaticamente
# Para parar o streaming, use: query.stop()

# --- Monitoramento do Streaming ---
# Ver o status do streaming
print(f"Streaming status: {query.status}")
print(f"Streaming progress: {query.lastProgress}")

# --- Consultar a Tabela Bronze ---
# Você pode consultar a tabela enquanto o streaming está rodando
spark.read.table(bronze_table_name).display()

# --- Consultas úteis ---
# Contar total de registros
spark.sql(f"SELECT COUNT(*) as total_records FROM {bronze_table_name}").display()

# Ver últimos registros inseridos
spark.sql(f"""
SELECT * FROM {bronze_table_name} 
ORDER BY ingestion_timestamp DESC 
LIMIT 10
""").display()

# Ver dados por região
spark.sql(f"""
SELECT regionid, COUNT(*) as user_count 
FROM {bronze_table_name} 
GROUP BY regionid 
ORDER BY user_count DESC
""").display()

# --- Limpeza ---
# Para parar o streaming:
# query.stop()

# Para remover a tabela Bronze:
# spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name}")

# Para remover o checkpoint:
# dbutils.fs.rm(checkpoint_location, recurse=True)
