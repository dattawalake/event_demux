# Databricks notebook source
# MAGIC %run ../common_params

# COMMAND ----------

# DBTITLE 1,Install Confluent Kafka Package
pip install confluent-kafka

# COMMAND ----------

# DBTITLE 1,Create Topic-Pipeline Mapping Table
#create topic- pipeline mapping table for dynamic allocation
crate_sql = f""" 
CREATE OR REPLACE TABLE {catalog}.{schema}.topic_pipeline_mappings (
    id LONG GENERATED ALWAYS AS IDENTITY,
    topic_name STRING,
    pipeline_id STRING,
    type STRING,
    event_type ARRAY<STRING>
)
"""

spark.sql(crate_sql)

# COMMAND ----------

# DBTITLE 1,Insert Sample Ad Events Topics for allocation
# Insert sample topic configuration for mapping event types to specific topics in the pipeline
mappings_values = f"""
INSERT INTO {catalog}.{schema}.topic_pipeline_mappings (topic_name, event_type, type) VALUES
('{event_types[0]}', NULL, 'topic_based'),
('{event_types[1]}', NULL, 'topic_based'),
('{event_types[2]}', NULL, 'topic_based'),
('{event_types[3]}', NULL, 'topic_based'),
('{event_types[4]}', NULL, 'topic_based'),
('{payload_based_topic_name}', array{tuple(event_types)}, 'payload_based')
"""

spark.sql(mappings_values)

# COMMAND ----------

# DBTITLE 1,Generate Random Ad Event Data
import json
import random
import time
from datetime import datetime, timedelta

def random_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

def random_timestamp(start, end):
    return int(random.randint(start, end))

def generate_ad_event(event_id):
    base_time = int(time.mktime(datetime(2025, 4, 25, 10, 52).timetuple()) * 1000)  # Current timestamp in ms
    timestamp = random_timestamp(base_time - 3600000, base_time)  # Within last hour
    
    
    event_type = random.choice(event_types)
    
    device_types = ["mobile", "desktop", "tablet"]
    os_types = ["iOS", "Android", "Windows", "macOS"]
    browsers = ["Safari", "Chrome", "Firefox", "Edge"]
    
    countries = ["US", "CA", "UK", "DE", "FR"]
    regions = {
        "US": ["CA", "NY", "TX", "WA", "FL"],
        "CA": ["ON", "BC", "QC"],
        "UK": ["England", "Scotland", "Wales"],
        "DE": ["Bavaria", "Berlin", "Hamburg"],
        "FR": ["Ile-de-France", "Provence", "Normandy"]
    }
    cities = {
        "CA": ["San Francisco", "Los Angeles", "San Diego"],
        "NY": ["New York", "Buffalo", "Albany"],
        "TX": ["Houston", "Dallas", "Austin"],
        "WA": ["Seattle", "Tacoma", "Spokane"],
        "FL": ["Miami", "Orlando", "Tampa"],
        "ON": ["Toronto", "Ottawa", "Hamilton"],
        "BC": ["Vancouver", "Victoria", "Kelowna"],
        "QC": ["Montreal", "Quebec City", "Gatineau"],
        "England": ["London", "Manchester", "Birmingham"],
        "Scotland": ["Edinburgh", "Glasgow", "Aberdeen"],
        "Wales": ["Cardiff", "Swansea", "Newport"],
        "Bavaria": ["Munich", "Nuremberg", "Augsburg"],
        "Berlin": ["Berlin"],
        "Hamburg": ["Hamburg"],
        "Ile-de-France": ["Paris", "Versailles"],
        "Provence": ["Marseille", "Nice", "Cannes"],
        "Normandy": ["Rouen", "Caen", "Le Havre"]
    }

    country = random.choice(countries)
    region = random.choice(regions[country])
    city = random.choice(cities.get(region, ["Unknown"]))

    event = {
        "eventId": f"ev-{event_id}",
        "timestamp": timestamp,
        "advertiserId": f"adv-{random.randint(1000, 9999)}",
        "campaignId": f"camp-{random.randint(1000, 9999)}",
        "adId": f"ad-{random.randint(1000, 9999)}",
        "eventType": event_type,
        "userId": f"user-{random.randint(1000, 9999)}",
        "deviceInfo": {
            "type": random.choice(device_types),
            "os": random.choice(os_types),
            "browser": random.choice(browsers),
            "ipAddress": random_ip()
        },
        "locationData": {
            "country": country,
            "region": region,
            "city": city
        },
        "contextData": {
            "publisherId": f"pub-{random.randint(1000, 9999)}",
            "placementId": f"plc-{random.randint(1000, 9999)}",
            "referrerUrl": "https://example.com/article"
        },
        "metrics": {
            "bidPrice": round(random.uniform(0.1, 1.0), 2),
            "chargedAmount": round(random.uniform(0.05, 0.95), 2),
            "viewabilityScore": round(random.uniform(0.5, 1.0), 2),
            "loadTime": random.randint(50, 500)
        } if event_type in ["adClick", "adImpression", "adView"] else None,
        "attributionData": {
            "clickId": f"click-{random.randint(1000, 9999)}" if event_type == "adClick" else None,
            "conversionId": f"conv-{random.randint(1000, 9999)}" if event_type == "adConversion" else None,
            "conversionValue": round(random.uniform(1.0, 100.0), 2) if event_type == "adConversion" else None,
            "conversionTime": timestamp + random.randint(1000, 100000) if event_type == "adConversion" else None
        } if event_type in ["adClick", "adConversion"] else None,
        "engagementData": {
            "engagementTime": random.randint(1, 300),
            "engagementType": random.choice(["like", "share", "comment"])
        } if event_type == "adEngagement" else None,
        "error_code": random.choice(["0", "1"]) 
    }
    return event


# COMMAND ----------

# DBTITLE 1,Produce and Send Events to Kafka Topics
from confluent_kafka import Producer

producer = Producer(**PRODUCER_CONF)

# Generate and send sample events
for i in range(1, 10):  
    event = generate_ad_event(i)
    serialized_event = json.dumps(event).encode('utf-8')

    #write to commbined topic all events to one topic i.e. palyload based demuxing
    producer.produce(payload_based_topic_name, value=serialized_event)
    
    # Optional: Add small delay to avoid overwhelming the broker
    time.sleep(0.01)

    #write usecase 2 indepedendt topics per event type i.e. topic based demuxing
    topic_name = event.get("eventType")
    producer.produce(topic_name, value=serialized_event)
      
    # Print progress
    if i % 100 == 0:
        print(f"Sent {i} events")

producer.flush()


# COMMAND ----------

# DBTITLE 1,Create Kafka Topics and Schema Config File
import pprint
import yaml
from pyspark.sql.types import StructType

topic_info = {}

import copy
topics = copy.deepcopy(event_types)
topics.append(payload_based_topic_name)

# Batch read from kafka for starting and ending offset to get sample of data for schema inference
for topic in set(topics):
   
    df = (spark.read
        .format("kafka")
        .options(**KAFKA_CONF) 
        .option("subscribe", topic )
        .option("startingOffsets", 'earliest' )
        .option("endingOffsets", 'latest')
        .load()
    )

    topic_schema = list(df.selectExpr("schema_of_json_agg(value::string) as schema").limit(1).toPandas()['schema'])[0]

    parsed_schema = StructType.fromDDL(topic_schema)

    valid_string_schema = ", ".join([ f"{field.name} {field.dataType.simpleString()}" for field in parsed_schema.fields])

    if topic == payload_based_topic_name:
        topic_info[topic] = {'type': 'payload_based', 'schemahint': valid_string_schema, 'event_type': event_types}
    else:
        topic_info[topic] = {'type': 'topic_based', 'schemahint': valid_string_schema}

with open('./pipeline/topic_schema_mapping.yaml', 'w') as file:
    yaml.dump(topic_info, file)

print(f"config creation completed: {catalog}.{schema}.topic_pipeline_mappings")
print(f"topic created with event data : {topics}")
print(f"topic-schema mappings file generated at ./pipeline/topic_schema_mapping.yaml")