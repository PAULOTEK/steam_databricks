# Setup Guide - Kafka Streaming with Databricks

## Prerequisites

1. **Databricks Workspace** - You need access to a Databricks workspace
2. **Kafka Cluster** - Confluent Cloud or self-hosted Kafka cluster
3. **External Storage** - S3 (AWS) or ADLS (Azure) for checkpoints and Delta tables
4. **Databricks Cluster** - With appropriate Spark configurations

## Kafka Configuration

### Confluent Cloud Setup

1. Go to [Confluent Cloud](https://confluent.cloud/)
2. Create a cluster or use existing one
3. Create API Key and Secret
4. Create topics (e.g., `sample_data_users`)
5. Note down:
   - Bootstrap servers (e.g., `pkc-xxxxx.eastus.azure.confluent.cloud:9092`)
   - API Key
   - API Secret
   - Topic names

### Update Notebook Parameters

Replace the placeholder values in each notebook:

```python
confluentBootstrapServers = "your-bootstrap-servers"
confluentTopicName = "your-topic-name"
confluentApiKey = "your-api-key"
confluentSecret = "your-api-secret"
```

## External Storage Setup

### AWS S3

1. Create an S3 bucket for checkpoints and Delta tables
2. Configure IAM role with appropriate permissions
3. Update notebook with:
   ```python
   checkpoint_location = "s3://your-bucket-name/checkpoints/your-checkpoint-name"
   ```

### Azure Data Lake Storage (ADLS)

1. Create a storage account and container
2. Configure access credentials
3. Update notebook with:
   ```python
   checkpoint_location = "abfss://container@storageaccount.dfs.core.windows.net/checkpoints/your-checkpoint-name"
   ```

## Databricks Cluster Configuration

### Required Libraries

Install these libraries on your cluster:

```json
{
  "libraries": [
    {
      "maven": {
        "coordinates": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "exclusions": ["org.slf4j:slf4j-log4j12"]
      }
    }
  ]
}
```

### Cluster Settings

- **Spark Version**: 3.5.x or higher
- **Scala Version**: 2.12
- **Python Version**: 3.10 or higher
- **Worker Type**: Based on your data volume
- **Driver Type**: Based on your requirements

## Notebook Usage

### 1. Basic Streaming (read_stream_from_kafka.py)

Use this to:
- Test your Kafka connection
- Verify data format
- Understand schema structure

### 2. Continuous Streaming to Bronze (kafka_to_bronze_streaming.py)

Use this to:
- Create a continuous streaming pipeline
- Ingest data to Bronze table
- Monitor real-time data flow

### 3. Delta Live Tables (kafka_dlt_bronze.py)

Use this to:
- Create automated production pipelines
- Implement data quality rules
- Manage schema evolution
- Simplify operations

### 4. Multiple Topics (multiple_kafka_topics_bronze.py)

Use this to:
- Process multiple Kafka topics simultaneously
- Create separate Bronze tables per topic
- Join data from different topics

### 5. Real-time Transformations (kafka_realtime_transformations.py)

Use this to:
- Apply transformations in real-time
- Add business logic during ingestion
- Create real-time metrics
- Monitor data quality

## Best Practices

### Checkpoint Management

- Use separate checkpoint locations for each streaming query
- Store checkpoints in the same region as your Databricks workspace
- Don't delete checkpoints unless you want to restart from beginning

### Schema Management

- Define schemas explicitly for better performance
- Use `mergeSchema` option for schema evolution
- Monitor schema changes in production

### Performance Optimization

- Use partitioning for large tables
- Enable auto-compaction and optimize write
- Set appropriate trigger intervals based on latency requirements
- Use watermarking for stateful operations

### Monitoring

- Monitor streaming query status regularly
- Check processing latency metrics
- Set up alerts for failures
- Review query progress and statistics

## Troubleshooting

### Connection Issues

```python
# Test Kafka connection
df_test = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", confluentBootstrapServers) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'...') \
    .option("subscribe", confluentTopicName) \
    .load()
```

### Schema Mismatch

- Verify your schema matches the Kafka topic data
- Check for nested JSON structures
- Use `display(df_raw)` to inspect raw data

### Performance Issues

- Increase cluster resources
- Adjust trigger intervals
- Review partitioning strategy
- Check for skew in data distribution

### Checkpoint Issues

- Ensure checkpoint location is accessible
- Check for sufficient storage space
- Verify IAM/permissions
- Delete and recreate checkpoint if corrupted

## Security Considerations

- Never commit API keys to version control
- Use Databricks Secrets for sensitive information
- Enable SSL/TLS for Kafka connections
- Implement proper IAM roles for storage access
- Use VPC endpoints for private connectivity

## Next Steps

1. Start with the basic example to verify connectivity
2. Progress to continuous streaming for production use
3. Implement Delta Live Tables for automated pipelines
4. Add data quality rules and monitoring
5. Scale to multiple topics and complex transformations

## Support

For issues or questions:
- Check Databricks documentation: https://docs.databricks.com/
- Kafka documentation: https://kafka.apache.org/documentation/
- Confluent Cloud docs: https://docs.confluent.io/