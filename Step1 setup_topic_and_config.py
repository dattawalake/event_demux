# Databricks notebook source
# MAGIC %md
# MAGIC ## Data and Configuration Environment
# MAGIC ### Purpose
# MAGIC - Video advertising tracking and performance optimization for ad technology applications
# MAGIC
# MAGIC ### Implementation Steps
# MAGIC - Establish Kafka topics and develop a synthetic data generator that publishes JSON-formatted events to these topics
# MAGIC - Create a YAML configuration file that maps topics to schemas by sampling topic data using "**schema_of_json_agg**"
# MAGIC - Build a configuration system table for dynamic allocation management of topic-to-pipeline.

# COMMAND ----------

# DBTITLE 1,Initiate Demux Config and Topic Data Setup
# MAGIC %run ./utils/setup_demuxconfig_and_topicdata