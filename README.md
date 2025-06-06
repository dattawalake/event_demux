# Event Demultiplexing

## Description
Demo explain how to implement event deuxing usecase using DLT declarative framework 

## Setup
### Prerequisites
- A functioning event system (e.g., Kafka) with appropriate access permissions.
- Access to a Databricks workspace configured with Unity Catalog, along with the necessary permissions to create and use catalogs and schemas.
### Instructions for setting up the Demo:
- Review all notebook code. Use Databricks Assistant for code comprehension.
- Set up required secrets and verify the "common_params" notebook for parameter details.
- Run the "Step1 setup_topic_and_config" notebook to create the demo topic, generate synthetic demo data, and set up the config table.
- Run the "Step2 install_topic_pipeline_allocation" notebook to create a new pipeline and allocate new topics to the newly created pipeline or existing pipeline.
- Execute the pipeline from the workspace UI once the new pipeline is installed.
- Finally, run the "Step3 cleanup_resources" notebook to release all resources created during the demo execution.
### Document references
- DLT  - https://docs.databricks.com/aws/en/dlt/
- FLOWS - https://docs.databricks.com/aws/en/dlt/flows
- SINK - https://docs.databricks.com/aws/en/dlt/dlt-sinks
