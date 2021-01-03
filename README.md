# problem_airflow_api_aggregation_pipeline

The pipeline to create and handle a scheduled dependancy based pipeline
1. Call api and format it into the api dump table
2. Dependent on 1 - Aggregate after reading the api dump table and inserting into the other aggregation table (postgres)
