# --- Kafka Streaming com Delta Live Tables ---
# Este notebook usa Delta Live Tables (DLT) para criar um pipeline de streaming
# DLT gerencia automaticamente os checkpoints, schema evolution e outras complexidades

# --- Parameters ---
confluentBootstrapServers = "pkc-*******.eastus.azure.confluent.cloud:9092"
confluentTopicName = "sample_data_users"
confluentApiKey = "*****************"
confluentSecret = "****************************************"

import dlt
from pyspark.sql.functions import col, from_json, expr, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType

# --- Bronze Table usando DLT ---
@dlt.table(
    name="bronze_users_kafka",
    comment="Tabela Bronze com dados brutos do Kafka em tempo real",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "bronze"
    }
)
def bronze_users_kafka():
    """
    Lê dados do Kafka e cria a tabela Bronze.
    DLT gerencia automaticamente os checkpoints e o streaming.
    """
    
    # Schema dos dados
    schema = StructType([
        StructField("registertime", LongType(), True),
        StructField("userid", StringType(), True),
        StructField("regionid", StringType(), True),
        StructField("gender", StringType(), True)
    ])
    
    # Configuração do Kafka
    kafka_options = {
        "kafka.bootstrap.servers": confluentBootstrapServers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluentApiKey}" password="{confluentSecret}";',
        "subscribe": confluentTopicName,
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
        from_json(col("clean_value"), schema).alias("data")
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
        lit(confluentTopicName)
    )
    
    return df_bronze

# --- Silver Table usando DLT ---
@dlt.table(
    name="silver_users_clean",
    comment="Tabela Silver com dados limpos e enriquecidos",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "quality": "silver"
    }
)
@dlt.expect("valid_userid", "userid IS NOT NULL")
@dlt.expect("valid_regionid", "regionid IS NOT NULL")
def silver_users_clean(bronze_users_kafka):
    """
    Processa e limpa os dados da tabela Bronze.
    Adiciona regras de qualidade de dados.
    """
    
    from pyspark.sql.functions import upper, trim
    
    df_silver = bronze_users_kafka.filter(
        col("userid").isNotNull() &
        col("regionid").isNotNull()
    ).withColumn(
        "gender_clean",
        upper(trim(col("gender")))
    ).withColumn(
        "regionid_clean",
        upper(trim(col("regionid")))
    ).withColumn(
        "processing_date",
        current_date()
    )
    
    return df_silver

# --- Gold Table usando DLT ---
@dlt.table(
    name="gold_users_metrics",
    comment="Tabela Gold com métricas agregadas",
    table_properties={
        "quality": "gold"
    }
)
def gold_users_metrics(silver_users_clean):
    """
    Cria métricas agregadas dos dados da tabela Silver.
    """
    
    from pyspark.sql.functions import count, current_timestamp
    
    df_gold = silver_users_clean.groupBy(
        "regionid_clean",
        "gender_clean",
        "processing_date"
    ).agg(
        count("*").alias("user_count"),
        count("*").alias("total_records")
    ).withColumn(
        "last_updated",
        current_timestamp()
    )
    
    return df_gold

# --- Como usar este pipeline DLT ---
# 1. Importe este notebook como parte de um pipeline Delta Live Tables
# 2. Configure o pipeline para rodar em modo contínuo (Continuous)
# 3. O DLT vai gerenciar automaticamente:
#    - Checkpoints
#    - Schema evolution
#    - Data quality expectations
#    - Monitoramento e alertas
#    - Orquestração das tabelas (dependências)

# --- Consultar as tabelas DLT ---
# Após o pipeline estar rodando, você pode consultar as tabelas:
# spark.read.table("bronze_users_kafka").display()
# spark.read.table("silver_users_clean").display()
# spark.read.table("gold_users_metrics").display()

# --- Benefícios do DLT ---
# - Gerenciamento automático de infraestrutura
# - Data quality expectations nativas
# - Schema evolution automático
# - Monitoramento integrado
# - Suporte a batch e streaming no mesmo pipeline
# - Deploy simplificado com interface visual
