# Databricks Series

Start your Databricks journey from the very basics and become a pro! This playlist covers everything you need to know—from foundational concepts to advanced data engineering, analytics, and AI—all using the powerful Databricks Lakehouse platform. Follow along with clear explanations and practical examples to boost your skills step-by-step.

Course link: https://youtube.com/playlist?list=PLjl2dJMjkDjkY_R9vv8WoZL6Dj7hIAtDT&si=dLoQAfVA8A4pzagU

Follow us:
* Join the channel: https://www.youtube.com/channel/UCwhERUcuzUCwr8x8mQ8zrcw/join
* YouTube: https://www.youtube.com/@TechWithYeshwanth/videos
* Follow our GitHub here: https://github.com/yeshwanthlm
* Follow our blog here: https://dev.to/yeshwanthlm/
* Follow us on Instagram: https://www.instagram.com/techwithyeshwanth/
* Follow us on LinkedIn: https://www.linkedin.com/in/yeshwanth-l-m/
* Book 1:1 Meeting with me: https://topmate.io/techwithyeshwanth

#Databricks #Lakehouse #DataEngineering #BigData #Analytics #DataLakes #DataWarehouse #MachineLearning #CloudComputing #DataScience #DataPlatform #TechWithYeshwanth

## Kafka Streaming Examples

This repository contains practical examples of streaming data pipelines using Kafka and Databricks:

### Notebooks

1. **read_stream_from_kafka.py** - Basic example of reading from Kafka topic and displaying data
2. **kafka_to_bronze_streaming.py** - Complete example of continuous streaming from Kafka to Bronze table
3. **kafka_dlt_bronze.py** - Delta Live Tables implementation for automated streaming pipeline
4. **multiple_kafka_topics_bronze.py** - Reading from multiple Kafka topics simultaneously
5. **kafka_realtime_transformations.py** - Real-time data transformations and enrichments

### Key Features

- **Continuous Streaming**: Process events in real-time as they arrive
- **Bronze Tables**: Raw data ingestion with metadata tracking
- **Delta Live Tables**: Automated pipeline management with data quality expectations
- **Multi-topic Support**: Handle multiple Kafka topics in parallel
- **Real-time Transformations**: Clean, validate, and enrich data on-the-fly
- **Watermarking**: Handle late-arriving data with windowed operations

### Quick Start

1. Configure your Kafka connection parameters in each notebook
2. Set up your external location for checkpoints (S3/ADLS)
3. Run the notebooks in Databricks
4. Monitor streaming queries and query Bronze tables in real-time

### Architecture

```
Kafka Topics → Spark Streaming → Bronze Tables → Silver/Gold Tables
     ↓                    ↓                ↓
  Events          Transformations      Analytics
```

For detailed production architecture, see:
- **ARCHITECTURE.md** - Complete production architecture documentation
- **architecture-drawio.xml** - Visual architecture diagram (import into Draw.io)
- **production-config-examples.py** - Production-ready configuration examples
