# Event Demultiplexing

## Description
Demo explain how to implement event demuxing usecase using DLT declarative framework 

## Important
Important information about Private Preview : some features used in demo are in private preview

- A product in either of these preview phases is not intended for use in production and is provided AS-IS consistent with your agreement with Databricks.
- Non-public information about the preview is confidential.
- We may change or discontinue the preview at any time without notice. We may also choose not to make the preview generally commercially available.
- These preview phases are not included in the SLA and do not have formal support. If you have questions or feedback, reach out to your Databricks contact.
- During Private Preview, Databricks may collect and use data relating to this feature, such as inputs and outputs, to develop and test the feature.
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
