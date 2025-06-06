# Databricks notebook source
# MAGIC %md
# MAGIC ### Clean up resources generated during demo execution
# MAGIC - Remove all created demo topics
# MAGIC - Remove the **schemas used** for the demo
# MAGIC - Remove generated pipelines
# MAGIC

# COMMAND ----------

# MAGIC %run ./utils/cleanup