# --- Production Configuration Examples ---
# This file contains production-ready configuration examples for the Kafka-Databricks streaming architecture

# --- 1. Cluster Configuration for Streaming ---
# Production cluster configuration optimized for streaming workloads

cluster_config = {
    "cluster_name": "kafka-streaming-production",
    "spark_version": "13.3.x-scala2.12",  # Latest stable version
    "autotermination_minutes": 0,  # Never auto-terminate for production
    "num_workers": 8,  # Start with 8 workers, auto-scale as needed
    "node_type_id": "i3.xlarge",  # Memory-optimized for streaming
    "driver_node_type_id": "i3.xlarge",
    "spark_conf": {
        # Streaming optimizations
        "spark.sql.streaming.checkpointLocation": "s3://your-bucket/checkpoints/",
        "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
        
        # Performance tuning
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        
        # Memory management
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.5",
        "spark.executor.memoryOverhead": "2g",
        
        # Delta Lake optimizations
        "spark.sql.execution.arrow.enabled": "true",
        "spark.delta.autoOptimize.optimizeWrite": "true",
        "spark.delta.autoOptimize.autoCompact": "true",
        
        # Kafka specific
        "spark.streaming.backpressure.enabled": "true",
        "spark.streaming.backpressure.initialRate": "10000",
        "spark.streaming.kafka.maxRatePerPartition": "1000",
        
        # Monitoring
        "spark.metrics.conf.*.sink.prometheus.class": "org.apache.spark.metrics.sink.PrometheusSink",
    },
    "custom_tags": {
        "Environment": "Production",
        "Team": "Data Engineering",
        "CostCenter": "Data Platform",
        "Project": "Kafka Streaming"
    }
}

# --- 2. Advanced Kafka Configuration with Retry Logic ---
# Production Kafka configuration with enhanced error handling

kafka_config_production = {
    "kafka.bootstrap.servers": "broker-1:9092,broker-2:9092,broker-3:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_API_KEY}" password="${KAFKA_API_SECRET}";',
    
    # Topic subscription with failover
    "subscribe": "users_events,orders_events,products_events",
    
    # Offset management
    "startingOffsets": "latest",  # For production, use latest
    "failOnDataLoss": "false",  # Don't fail on data loss in production
    
    # Consumer configuration
    "kafka.consumer.fetch.max.bytes": "52428800",  # 50MB
    "kafka.consumer.max.partition.fetch.bytes": "1048576",  # 1MB
    "kafka.consumer.session.timeout.ms": "30000",
    "kafka.consumer.heartbeat.interval.ms": "10000",
    "kafka.consumer.max.poll.records": "500",
    "kafka.consumer.max.poll.interval.ms": "300000",
    
    # SSL/TLS configuration
    "kafka.ssl.protocol": "TLSv1.2",
    "kafka.ssl.enabled.protocols": "TLSv1.2,TLSv1.3",
    
    # Security
    "kafka.consumer.enable.auto.commit": "false"  # Let Spark manage offsets
}

# --- 3. Delta Table Configuration with Production Features ---
# Production Delta table configuration

delta_table_config = {
    "bronze": {
        "table_properties": {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.checkpoint.writeStatsAsJson": "false",
            "delta.compressionCodec": "zstd",  # Better compression
            "delta.dataSkippingNumIndexedCols": "32",
            "delta.deletedFileRetentionDuration": "interval 7 days",
            "delta.logRetentionDuration": "interval 30 days",
            "delta.enableChangeDataFeed": "true",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
            "delta.tuneFileSizesForRewrites": "true"
        },
        "partitioning": ["ingestion_date", "topic"],
        "zorder_by": ["ingestion_timestamp"]
    },
    "silver": {
        "table_properties": {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.compressionCodec": "zstd",
            "delta.dataSkippingNumIndexedCols": "32",
            "delta.deletedFileRetentionDuration": "interval 30 days",
            "delta.logRetentionDuration": "interval 90 days",
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true"
        },
        "partitioning": ["region", "processing_date"],
        "zorder_by": ["event_timestamp", "user_id"]
    },
    "gold": {
        "table_properties": {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.compressionCodec": "zstd",
            "delta.dataSkippingNumIndexedCols": "32",
            "delta.deletedFileRetentionDuration": "interval 90 days",
            "delta.logRetentionDuration": "interval 365 days",
            "delta.enableChangeDataFeed": "true",
            "delta.enableDeletionVectors": "true"
        },
        "partitioning": ["date", "region"],
        "zorder_by": ["metric_timestamp"]
    }
}

# --- 4. Data Quality Rules for Production ---
# Comprehensive data quality rules

data_quality_rules = {
    "bronze_layer": {
        "expectations": [
            {
                "name": "valid_json",
                "expectation": "value IS NOT NULL",
                "action": "drop"
            },
            {
                "name": "valid_timestamp",
                "expectation": "ingestion_timestamp IS NOT NULL",
                "action": "drop"
            },
            {
                "name": "valid_source",
                "expectation": "source IN ('kafka', 'api', 'batch')",
                "action": "drop"
            }
        ],
        "metrics": [
            "total_records",
            "valid_records",
            "invalid_records",
            "quality_score"
        ]
    },
    "silver_layer": {
        "expectations": [
            {
                "name": "no_null_ids",
                "expectation": "user_id IS NOT NULL AND order_id IS NOT NULL",
                "action": "drop"
            },
            {
                "name": "valid_regions",
                "expectation": "region IN ('US', 'EU', 'APAC', 'LATAM')",
                "action": "drop"
            },
            {
                "name": "positive_quantities",
                "expectation": "quantity > 0",
                "action": "drop"
            },
            {
                "name": "valid_dates",
                "expectation": "event_date >= '2020-01-01'",
                "action": "drop"
            }
        ],
        "metrics": [
            "total_records",
            "quality_violations",
            "duplicate_records",
            "data_freshness"
        ]
    },
    "gold_layer": {
        "expectations": [
            {
                "name": "no_null_metrics",
                "expectation": "revenue >= 0 AND orders >= 0",
                "action": "drop"
            },
            {
                "name": "reasonable_values",
                "expectation": "revenue < 1000000",  # Business rule
                "action": "alert"
            }
        ],
        "metrics": [
            "total_revenue",
            "total_orders",
            "unique_customers",
            "data_completeness"
        ]
    }
}

# --- 5. Monitoring and Alerting Configuration ---
# Production monitoring setup

monitoring_config = {
    "alerts": {
        "streaming": [
            {
                "name": "high_processing_latency",
                "condition": "processing_latency_p95 > 5000",  # 5 seconds
                "severity": "warning",
                "notification": ["slack", "email"]
            },
            {
                "name": "consumer_lag_high",
                "condition": "consumer_lag > 100000",
                "severity": "critical",
                "notification": ["pagerduty", "slack"]
            },
            {
                "name": "streaming_failure",
                "condition": "stream_status == 'failed'",
                "severity": "critical",
                "notification": ["pagerduty", "slack", "email"]
            },
            {
                "name": "high_error_rate",
                "condition": "error_rate > 0.01",  # 1%
                "severity": "warning",
                "notification": ["slack"]
            }
        ],
        "infrastructure": [
            {
                "name": "cluster_overloaded",
                "condition": "cluster_utilization > 0.9",
                "severity": "warning",
                "notification": ["slack"]
            },
            {
                "name": "storage_capacity_warning",
                "condition": "storage_usage > 0.8",
                "severity": "warning",
                "notification": ["email"]
            },
            {
                "name": "api_latency_high",
                "condition": "api_latency_p95 > 1000",
                "severity": "warning",
                "notification": ["slack"]
            }
        ],
        "business": [
            {
                "name": "data_freshness_issue",
                "condition": "data_age > 300",  # 5 minutes
                "severity": "warning",
                "notification": ["slack"]
            },
            {
                "name": "quality_score_low",
                "condition": "quality_score < 0.95",
                "severity": "warning",
                "notification": ["email"]
            }
        ]
    },
    "dashboards": {
        "streaming_health": {
            "metrics": [
                "processing_latency",
                "throughput",
                "consumer_lag",
                "error_rate",
                "stream_status"
            ],
            "refresh_interval": "30s"
        },
        "data_quality": {
            "metrics": [
                "quality_score",
                "validation_violations",
                "duplicate_rate",
                "completeness_score"
            ],
            "refresh_interval": "1m"
        },
        "infrastructure": {
            "metrics": [
                "cluster_utilization",
                "storage_usage",
                "network_throughput",
                "api_latency"
            ],
            "refresh_interval": "1m"
        }
    }
}

# --- 6. CI/CD Pipeline Configuration ---
# Production deployment pipeline

cicd_config = {
    "stages": {
        "development": {
            "cluster": "dev-cluster",
            "environment": "dev",
            "schedule": "manual",
            "approval": false
        },
        "staging": {
            "cluster": "staging-cluster",
            "environment": "staging",
            "schedule": "triggered",
            "approval": true,
            "tests": [
                "unit_tests",
                "integration_tests",
                "performance_tests"
            ]
        },
        "production": {
            "cluster": "prod-cluster",
            "environment": "production",
            "schedule": "continuous",
            "approval": true,
            "tests": [
                "unit_tests",
                "integration_tests",
                "performance_tests",
                "security_scan",
                "data_quality_tests"
            ],
            "rollback": true
        }
    },
    "deployment_strategy": "blue_green",
    "health_checks": [
        "stream_status",
        "data_freshness",
        "quality_score",
        "consumer_lag"
    ]
}

# --- 7. Error Handling and Retry Logic ---
# Production error handling

error_handling_config = {
    "retry_policy": {
        "max_retries": 3,
        "backoff_multiplier": 2,
        "initial_delay": 1000,  # 1 second
        "max_delay": 60000,  # 1 minute
        "retryable_errors": [
            "TimeoutError",
            "ConnectionError",
            "RateLimitError",
            "TemporaryFailure"
        ]
    },
    "dead_letter_queue": {
        "enabled": true,
        "table": "bronze.dead_letter_queue",
        "max_age": "30 days",
        "include_metadata": True
    },
    "circuit_breaker": {
        "enabled": True,
        "failure_threshold": 5,
        "success_threshold": 2,
        "timeout": 60000,
        "half_open_max_calls": 3
    }
}

# --- 8. Cost Optimization Configuration ---
# Production cost optimization

cost_optimization_config = {
    "cluster_policies": {
        "auto_scaling": {
            "enabled": True,
            "min_workers": 4,
            "max_workers": 20,
            "scale_up_cooldown": 300,
            "scale_down_cooldown": 300
        },
        "spot_instances": {
            "enabled": True,
            "spot_percentage": 0.7,  # 70% spot instances
            "fallback_to_on_demand": True
        },
        "auto_termination": {
            "non_production": {
                "enabled": True,
                "idle_timeout": 30  # minutes
            },
            "production": {
                "enabled": False
            }
        }
    },
    "storage_optimization": {
        "vacuum_schedule": "weekly",
        "retention_policies": {
            "bronze": "7 days",
            "silver": "30 days",
            "gold": "365 days"
        },
        "compression": "zstd",
        "file_size_target": "128MB"
    }
}

# --- 9. Security Configuration ---
# Production security settings

security_config = {
    "encryption": {
        "at_rest": {
            "enabled": True,
            "algorithm": "AES-256",
            "key_management": "kms"
        },
        "in_transit": {
            "enabled": True,
            "protocol": "TLS 1.2+",
            "cipher_suites": ["TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"]
        }
    },
    "access_control": {
        "unity_catalog": {
            "enabled": True,
            "privileges": {
                "bronze": ["SELECT", "CREATE"],
                "silver": ["SELECT", "CREATE", "UPDATE"],
                "gold": ["SELECT"]
            }
        },
        "row_level_security": {
            "enabled": True,
            "policies": {
                "region_based": "region = user_region",
                "data_classification": "classification_level <= user_clearance"
            }
        }
    },
    "audit_logging": {
        "enabled": True,
        "log_level": "INFO",
        "retention": "365 days",
        "events": [
            "data_access",
            "schema_changes",
            "cluster_operations",
            "job_runs"
        ]
    }
}

# --- 10. Disaster Recovery Configuration ---
# Production disaster recovery

disaster_recovery_config = {
    "backup_strategy": {
        "delta_time_travel": {
            "enabled": True,
            "retention_period": "90 days"
        },
        "cross_region_replication": {
            "enabled": True,
            "source_region": "us-east-1",
            "destination_region": "us-west-2",
            "replication_frequency": "continuous"
        },
        "checkpoint_backups": {
            "enabled": True,
            "schedule": "daily",
            "retention": "30 days"
        }
    },
    "recovery_objectives": {
        "rto": "15 minutes",  # Recovery Time Objective
        "rpo": "1 minute",   # Recovery Point Objective
        "data_loss_tolerance": "0 events"
    },
    "failover": {
        "automatic": True,
        "health_check_interval": 30,  # seconds
        "failover_conditions": [
            "primary_region_unavailable",
            "replication_lag > 5 minutes",
            "error_rate > 5%"
        ]
    }
}

# --- Usage Example ---
# Example of how to use these configurations in production

def setup_production_pipeline():
    """
    Setup a production-ready streaming pipeline using the configurations above
    """
    
    # Configure cluster
    cluster = create_cluster(cluster_config)
    
    # Setup Kafka connection with retry logic
    kafka_reader = (
        spark.readStream
        .format("kafka")
        .options(**kafka_config_production)
        .load()
    )
    
    # Apply data quality rules
    bronze_df = apply_data_quality(kafka_reader, data_quality_rules["bronze_layer"])
    
    # Write to Bronze with Delta optimizations
    bronze_writer = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3://your-bucket/checkpoints/bronze")
        .options(**delta_table_config["bronze"]["table_properties"])
        .partitionBy(*delta_table_config["bronze"]["partitioning"])
        .toTable("bronze.streaming_events")
    )
    
    # Setup monitoring
    setup_monitoring(monitoring_config)
    
    # Configure error handling
    setup_error_handling(error_handling_config)
    
    return bronze_writer

# This configuration file provides a comprehensive foundation for production deployment
# Adjust parameters based on your specific requirements and environment