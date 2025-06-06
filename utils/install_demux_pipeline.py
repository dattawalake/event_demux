# Databricks notebook source
# MAGIC %run ../common_params

# COMMAND ----------

# DBTITLE 1,Configure Kafka and Databricks Pipeline Settings
import uuid

# COMMAND ----------

# DBTITLE 1,Analyze Pipeline Allocation and Unallocated Topics
topic_pipeline_allocation_quey = f"""
SELECT pipeline_id, COUNT(*) AS count
FROM {catalog}.{schema}.topic_pipeline_mappings
WHERE pipeline_id IS NOT NULL
GROUP BY pipeline_id
HAVING count < {streams_per_pipeline}
ORDER BY count DESC
"""

unallocated_topic_query = f"""
SELECT topic_name AS topic
FROM {catalog}.{schema}.topic_pipeline_mappings
WHERE pipeline_id IS NULL
"""

# COMMAND ----------

# DBTITLE 1,Allocate newly added Topics to Pipelines
def allocate_topics_to_pipelines():
    topic_allocated_pipelines = spark.sql(topic_pipeline_allocation_quey)
    unallocated_topics = spark.sql(unallocated_topic_query)

    allocated_pipelines_pd = topic_allocated_pipelines.toPandas()
    unallocated_topics_pd = unallocated_topics.toPandas()

    pipeline_counts = dict(
        zip(allocated_pipelines_pd["pipeline_id"], allocated_pipelines_pd["count"])
    )

    update_list = []
    unallocated_topic_list = unallocated_topics_pd["topic"].tolist()
    eligible_pipelines = allocated_pipelines_pd["pipeline_id"].tolist()

    while unallocated_topic_list and eligible_pipelines:
        for pipeline_id in list(eligible_pipelines):
            if not unallocated_topic_list:
                break

            topic = unallocated_topic_list.pop(0)
            update_list.append({"pipeline_id": pipeline_id, "topic_name": topic})
            pipeline_counts[pipeline_id] += 1

            if pipeline_counts[pipeline_id] >= streams_per_pipeline:
                eligible_pipelines.remove(pipeline_id)

    leftover_topics = unallocated_topic_list

    print(f"Topics allocated in round-robin fashion: {len(update_list)}")
    print(f"\nLeftover topics that couldn't be allocated: {len(leftover_topics)}")
    print(leftover_topics)
    return update_list, leftover_topics

update_list, leftover_topics = allocate_topics_to_pipelines()

def update_topic_pipeline_mappings(update_list):
    # Create a temporary view for the update list
    update_df = spark.createDataFrame(update_list)
    update_df.createOrReplaceTempView("update_list_view")

    # Perform the update in one transaction
    spark.sql(
        f"""
    MERGE INTO {catalog}.{schema}.topic_pipeline_mappings AS target
    USING update_list_view AS source
    ON target.topic_name = source.topic_name
    WHEN MATCHED THEN
      UPDATE SET target.pipeline_id = source.pipeline_id
    """
    )

if len(update_list) > 0:
    update_topic_pipeline_mappings(update_list)

# COMMAND ----------

# DBTITLE 1,Create New Pipelines for Unallocated Topics

dlt_nb_path = notebook_context.notebookPath().getOrElse(None).replace("Step2 install_topic_pipeline_allocation", "pipeline/dlt_event_demux")
yml_file_path = "./topic_schema_mapping.yaml"


# Process leftover list and create one pipeline per max stream limit
def create_pipelines_for_leftover_topics(leftover_topics):
    new_pipelines = []
    pipeline_name_suffix = 1
    while leftover_topics:
        new_pipeline_topics = leftover_topics[:streams_per_pipeline]
        leftover_topics = leftover_topics[streams_per_pipeline:]
        PIPELINE_NAME = f"dais_demo_event_demux_usecase_{pipeline_name_suffix}"
        new_pipeline_payload = {
            "name": PIPELINE_NAME,
            "catalog": catalog,
            "schema": schema,
            "configuration": {
                "topic_names": f"{','.join(new_pipeline_topics)}",
                "yml_file_path": yml_file_path,
                'kafka_bootstrap_servers': KAFKA_SERVER,
                'sink_catalog': catalog,
                'sink_schema': schema
                
            },
            "development": True,
            "continuous": False,
            "channel": "PREVIEW",
            "libraries": [{"notebook": {"path": dlt_nb_path}}],
            "clusters": [
                {
                    "num_workers": 1,
                    "node_type_id": "i3.xlarge",
                    "driver_node_type_id": "i3.xlarge",
                    "aws_attributes": {
                        "instance_profile_arn": instance_profile_arn
                    },
                    "autoscale": {
                        "min_workers": 1,
                        "max_workers": 1,
                        "mode": "ENHANCED"
                    }
                }
            ]
        }

        response = pipelines_api.create(settings=new_pipeline_payload, allow_duplicate_names=False, settings_dir=None)
        print(f"New pipeline created: {response} for topics: {new_pipeline_topics} ")
        pipeline_name_suffix = pipeline_name_suffix +1 
        for topic in new_pipeline_topics:
            update_list.append({"pipeline_id": response['pipeline_id'], "topic_name": topic})

    return update_list

update_list = create_pipelines_for_leftover_topics(leftover_topics)

if len(update_list) > 0:
    update_topic_pipeline_mappings(update_list)