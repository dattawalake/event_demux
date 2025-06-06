# Databricks notebook source
pip install databricks_cli

# COMMAND ----------

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.pipelines.api import PipelinesApi

# COMMAND ----------

# DBTITLE 1,Create Catalog Based on Current User
my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

catalog = f"Demux_Demo_{my_name}"
schema = f"Adtech_demo_{my_name}"

# Ensure you have the necessary permissions to create a catalog. If not, modify the CATALOG and TARGET_SCHEMA variables accordingly.
spark.sql(f"create catalog if not exists {catalog}")
spark.sql(f"create schema if not exists {catalog}.{schema}")



# COMMAND ----------

# DBTITLE 1,Set Widgets for Pipeline Streams and AWS Profile
dbutils.widgets.text("streams_per_pipeline", "5")
dbutils.widgets.text("instance_profile_arn", "arn:aws:iam::997819012307:instance-profile/one-env-databricks-access")

# COMMAND ----------

# DBTITLE 1,Configure Kafka Server and Event Types for Processing
# Retrieve the Kafka bootstrap server address from Databricks secrets, replace with your own parameters
KAFKA_SERVER = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-plaintext")

streams_per_pipeline = int(dbutils.widgets.get("streams_per_pipeline"))
instance_profile_arn = dbutils.widgets.get("instance_profile_arn")

# Example event types for payload-based event processing, also used as topic names for topic-based event processing
event_types = ["adClick", "adImpression", "adConversion", "adView", "adEngagement"]
payload_based_topic_name = "all_adevents"

KAFKA_CONF = {
  "kafka.bootstrap.servers": f"{KAFKA_SERVER}",
  "kafka.sasl.mechanisms": "PLAIN"
}  

PRODUCER_CONF = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
 }

# COMMAND ----------

notebook_context = dbutils.entry_point.getDbutils().notebook().getContext()
api_client = ApiClient(token=notebook_context.apiToken().get(), host=notebook_context.apiUrl().get())
pipelines_api = PipelinesApi(api_client)