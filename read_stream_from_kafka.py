# --- Parameters ---
confluentBootstrapServers = "pkc-*******.eastus.azure.confluent.cloud:9092"
confluentTopicName = "sample_data_users"
confluentApiKey = "*****************"
confluentSecret = "****************************************"

# --- Read Stream from Kafka ---
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("registertime", LongType(), True),
    StructField("userid", StringType(), True),
    StructField("regionid", StringType(), True),
    StructField("gender", StringType(), True)
])

kafka_options = {
    "kafka.bootstrap.servers": confluentBootstrapServers,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="*****************" password="****************************************";',
    "subscribe": confluentTopicName,
    "startingOffsets": "earliest",
}

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Remove non-JSON leading characters from the value field
df_clean = df_raw.withColumn(
    "clean_value",
    expr("regexp_replace(CAST(value AS STRING), '^[^\\{]*', '')")
)

# Parse the cleaned JSON string
df_parsed = df_clean.select(
    from_json(col("clean_value"), schema).alias("data")
).select("data.*")

display(
    df_parsed,
    checkpointLocation = "s3://YOUR_EXTERNAL_LOCATION_BUCKET_NAME/checkpoints/sample_data_users"
)
