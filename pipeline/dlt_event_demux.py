# Databricks notebook source
pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Use Case1 : Topic-Based Demultiplexing
# MAGIC - Deseralized events using from_json with sampling file schema
# MAGIC - Events within a topic are routed based on topic and create target table (silver) per topic
# MAGIC
# MAGIC ## Use Case2 : Payload-Based Demultiplexing
# MAGIC - All raw events are stored into bronze table partitioned by 'eventType'
# MAGIC - Deseralized events using from_json with sampling file schema
# MAGIC - Events within a topic are routed based on event type atrribute (scenario 1: Bronze to Silver)
# MAGIC
# MAGIC ## Use Case3 : Handle quarantine requirments for faulty/errored events
# MAGIC - Foreach Batch Sink to process complex events e.g. quarantine faulty events (scenario 2: Bronze to Silver)
# MAGIC
# MAGIC ## Use Case4 : Kafka as Sink
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configure Kafka Topics and Schemas for Events
import yaml

kafka_bootstrap_servers = spark.conf.get("kafka_bootstrap_servers")
yml_file_path = spark.conf.get("yml_file_path")
topic_names = spark.conf.get("topic_names").split(',')
sink_catalog = spark.conf.get("sink_catalog")
sink_schema = spark.conf.get("sink_schema")

with open(yml_file_path, 'r') as f:
	topic_schema_mapping = yaml.load(f, Loader=yaml.SafeLoader)

# COMMAND ----------

# DBTITLE 1,Kafka to Bronze
import dlt

def create_bronze_tables(topic, schemaHint, table_name, type="topic_based"):
    decode_value_column = "decodeValue" if type == "payload_based" else "decodeValue.*"
    decode_value_event_type = ", decodeValue.eventType" if type == "payload_based" else ""
    
    @dlt.table(
        name=f"{table_name}",
        comment="Raw data from Kafka topics, Json payloads deserialized as Spark Struct",
        spark_conf={"pipelines.trigger.interval": "30 seconds"},
        partition_cols=["eventType"] if type == "payload_based" else [],
    )
    def bronze():
        return spark.sql(
            f"""
        SELECT 
          key,
          topic,
          partition,
          offset,
          timestamp as eventTime,
          timestampType,
          {decode_value_column}
          {decode_value_event_type}
        FROM 
        (
          SELECT
            *,
            from_json(value::string, NULL, map('schemaLocation', 'value_schema_{table_name}_{topic}', 'schemaHints', '{schemaHint}')) decodeValue
          FROM 
            STREAM READ_KAFKA(bootstrapServers => '{kafka_bootstrap_servers}', subscribe => '{topic}', startingOffsets => 'earliest')
        ) packed
      """)

# COMMAND ----------

# DBTITLE 1,Bronze to Silver Fan Out "Demux"
def create_silver_tables(source_table, target_table, event_type):
    #Scenario 1: Create table per event type or topic with simple projection from bronze
    @dlt.table(name=f"silver_{target_table}")
    def silver_table():
        return spark.sql(
            f"""
        SELECT 
          key,
          topic,
          eventTime,
          decodeValue.*
        FROM 
          STREAM({source_table}) where decodeValue.eventType = '{event_type}'
      """
        )

# COMMAND ----------

# DBTITLE 1,ForEachBatch Sink for Complex processing
def create_feb_sink(source_table, target_table, event_type):
    #Scenario 2: Complex event transformation e.g. handling quarantining faulty events requirment.
    sinkname = f"silver_feb_{target_table}"
    
    @dlt.foreach_batch_sink(name=sinkname)
    def process_batch(batch_df, batch_id):
        #write valid events to delta table
        valid_event = batch_df.filter(batch_df.error_code == 0)
        (valid_event.write.format("delta")
            .option("txnAppId", sinkname)
            .option("txnVersion", batch_id)  
            .option("mergeSchema", "true")
            .mode("append").saveAsTable(f"{sink_catalog}.{sink_schema}.{sinkname}")
        )
        
        #write error events to kafka quarantine topic
        faulty_event = batch_df.filter(batch_df.error_code != 0)
        faulty_event.selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("topic", "quarantine_events").save()

    # Attach the foreachBatch sink to a flow
    @dlt.append_flow(target=sinkname, name=f"flow_{event_type}")
    def read_bronze_data():
        return spark.sql(
            f"""
        SELECT 
          key,
          topic,
          eventTime,
          decodeValue.*
        FROM 
          STREAM({source_table}) where decodeValue.eventType = '{event_type}'
      """
        )

# COMMAND ----------

# DBTITLE 1,Create Kafka Sink
def create_kafka_sink(source_table, event_type):
  sink_table_name = f"kafka_sink_{event_type}"
  dlt.create_sink(
  name = sink_table_name,
  format = "kafka",
  options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "topic": sink_table_name
  }
  )

  # Attach the foreachBatch sink to a flow
  @dlt.append_flow(target=sink_table_name, name=f"kafka_flow_{event_type}")
  def read_bronze_data_for_kafka_sink():
        return spark.sql(
            f"""
        SELECT 
          key,
          topic,
          eventTime,
          decodeValue.*
        FROM 
          STREAM({source_table}) where decodeValue.eventType = '{event_type}'
      """
        ).selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")


# COMMAND ----------

# DBTITLE 1,Create Bronze Tables for Topic-Based Data
#Use Case 1 : Simple independent tables per topic
for topic, info in {k: v for k, v in topic_schema_mapping.items() if k in topic_names}.items():
  if info['type'] == 'topic_based':
    create_bronze_tables(topic, info['schemahint'], f'silver_{topic}', info['type'])

# COMMAND ----------

# DBTITLE 1,Dynamic Table Creation Based on Topic Schema
  
#Use Case 2 : Fat topic "payload-based" create tables per eventype
for topic, info in {k: v for k, v in topic_schema_mapping.items() if k in topic_names}.items():
  if info['type'] == 'payload_based':
    create_bronze_tables(topic, info['schemahint'], f'bronze_{topic}', info['type'])
    for event_type in info['event_type']:
      create_silver_tables(f'bronze_{topic}', f'{event_type}_{topic}', event_type)
      
      
#Use Case 3: Complex event processing e.g. quarantine faulty events
for topic, info in {k: v for k, v in topic_schema_mapping.items() if k in topic_names}.items():
  if info['type'] == 'payload_based':
    create_bronze_tables(topic, info['schemahint'], f'bronze_feb_{topic}', info['type'])
    for event_type in info['event_type']:
      create_feb_sink(f'bronze_feb_{topic}', f'{event_type}_{topic}', event_type)

#Use Case 4: Sink to Kafka
for topic, info in {k: v for k, v in topic_schema_mapping.items() if k in topic_names}.items():
  if info['type'] == 'payload_based':
    for event_type in info['event_type']:
      create_kafka_sink(f'bronze_{topic}', event_type)


# COMMAND ----------

# MAGIC %md
# MAGIC