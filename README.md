Environment:

Docker containers were used for this project. MinIO(compose file service name: minio), Spark(compose file service name: spark_client) and Airflow(compose file service name: airflow_scheduler) were separate services in the docker-compose file.

Content:

VBO Data Engineering Bootcamp Final Project-4: Airflow/Delta Lake

- Use this dataset: https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip

There are two different datasets in this zip file.

- tmdb_5000_credits.csv
- tmdb_5000_movies.csv

These datasets come into object storage at regular intervals in a batch manner.

1. Data Ingestion

- At this stage, the datasets should be generated into the `tmdb-bronze` bucket representing the bronze layer (with data-generator).
- You can use MinIO as object storage with docker.
- Generate data-generator datasets into `tmdb-bronze`. Example commands are below.

2. Data transformation

At this stage, the raw data in the bronze layer is converted and written into the silver layer `tmdb-silver/<table_name>` bucket in the form of delta tables.

3. Pipeline

- Create a pipeline with Airflow to meet the above requirements.
- Pipeline should run daily.
