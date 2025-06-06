# Databricks notebook source
# MAGIC %run ../common_params

# COMMAND ----------

pip install confluent-kafka

# COMMAND ----------

# DBTITLE 1,Delete Kafka Topics Based on Event Types

from confluent_kafka.admin import AdminClient

admin_client = AdminClient(PRODUCER_CONF)

topics = event_types
topics.append(payload_based_topic_name)
try:
  admin_client.delete_topics(topics)
except Exception as e:
  print(f"ERROR: {e}")


# COMMAND ----------

# DBTITLE 1,Remove Pipelines with Non-Null IDs

allocated_pipelines = f"""
SELECT pipeline_id
FROM {catalog}.{schema}.topic_pipeline_mappings
WHERE pipeline_id IS NOT NULL
"""
try:
  pipeline_ids = [row.pipeline_id for row in spark.sql(allocated_pipelines).collect()]
  
  for pipeline_id in pipeline_ids:
    try:
      pipelines_api.delete(pipeline_id)
    except Exception as e:
      print(f"ERROR: Failed to delete pipeline {pipeline_id}: {e}")
except Exception as e:
  print(f"ERROR: {e}")


# COMMAND ----------

# DBTITLE 1,Drop Database if Exists for Given Catalog and Schema
spark.sql(f"drop database if exists {catalog}.{schema} cascade")