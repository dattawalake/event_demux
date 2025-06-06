# Databricks notebook source
# MAGIC %md
# MAGIC ## Configure Demux DLT Pipelines
# MAGIC - Set up a new DLT pipeline for topics that have not yet been assigned <!-- Ensure all topics are accounted for -->
# MAGIC - Assign unallocated topics to existing pipelines, provided the stream limits for each pipeline are not surpassed <!-- Verify pipeline capacity before assignment -->
# MAGIC - Monitor and manage the allocation of topics to pipelines, including creating or updating pipelines as necessary <!-- Keep a record of topic-to-pipeline mappings -->
# MAGIC - Deploy and configure new pipelines when required <!-- Install and set up new pipelines as needed -->

# COMMAND ----------

# DBTITLE 1,Initiate Demux Config and Topic Data Setup
# MAGIC %run ./utils/install_demux_pipeline