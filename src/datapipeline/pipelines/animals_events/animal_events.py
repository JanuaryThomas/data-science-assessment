"""
Animal Events Data Pipeline

This module implements a data pipeline for processing animal events data using Apache Spark,
Delta Lake, and the Medallion Architecture. It reads data from a bronze layer (raw data) in
Amazon Athena and writes it to a silver layer (processed data) in Delta Lake format.

Key Components:
1. AnimalsEvents: A Pipeline subclass that orchestrates the ETL process.
2. Configuration: Source and target configurations for data reading and writing.
3. AWS Integration: Utilizes AWS Athena for data source and S3 for data storage.

Dependencies:
- Apache Spark
- Delta Lake
- AWS Athena
- Custom datapipeline library

Usage:
To run the pipeline, execute the main() function. This will initialize the AnimalsEvents
pipeline with the specified configurations and run the ETL process.

Classes:
    AnimalsEvents(Pipeline): Implements the ETL logic for animal events data.

Functions:
    main(): Entry point for running the animal events data pipeline.

Configuration:
    spark_extra_config: A list of Spark configuration options required for the pipeline.

Notes:
- Ensure that AWS credentials are properly set up and accessible.
- The pipeline uses Delta Lake for the target, enabling ACID transactions and time travel capabilities.
"""

from typing import List
from datapipeline.api.reader import Read
from datapipeline.api.writer import Writer
from delta.tables import DataFrame
from pyspark.sql import DataFrame
from datapipeline.api.pipeline import Pipeline
from datapipeline.pipelines.animals_events.sqls import (
    read_animal_event_from_bronze,
    write_animal_event_to_silver,
)

from datapipeline.utils.aws_utils import get_secrets

access_key = ""
secret_key = ""
s3_output_location = ""

athena_crdentials = get_secrets("ml_athena_secret")
if athena_crdentials:
    access_key = athena_crdentials["access_key"]
    secret_key = athena_crdentials["secret_key"]
    s3_output_location = athena_crdentials["s3_output_location"]


class AnimalsEvents(Pipeline):
    """
    AnimalsEvents Pipeline

    This class extends the base Pipeline class to implement the specific ETL logic
    for processing animal events data.

    Methods:
        extract(): Reads data from the source (Athena) into a Spark DataFrame.
        load(): Writes the processed data to the target (Delta Lake).
        run(): Orchestrates the full ETL process.
        __commit__(): (Not implemented) Placeholder for commit logic.
    """

    def __init__(self, pipeline_name: str, source_config: dict, target_config: dict):
        super().__init__(
            pipeline_name=pipeline_name,
            source_config=source_config,
            target_config=target_config,
            spark_extra_configs=spark_extra_config,
        )
        self.source_dataset: DataFrame = None

    def extract(self, header: bool = False) -> DataFrame:
        animals_events_data_reader: Read = Read(
            source_config=self.source_config,
            spark=self.spark,
        )
        animals_events_datafarame, _ = animals_events_data_reader.read()
        self.source_dataset = animals_events_datafarame

    def load(self) -> DataFrame:
        animals_events_data_writer: Writer = Writer(spark=self.spark)
        animals_events_data_writer.write(
            target_config=self.target_config, source_dataframe=self.source_dataset
        )
        self.logger.info("Data loaded successfully")

    def run(self):
        self.extract()
        self.load()

    def __commit__(self):
        pass


def main():
    options = {
        "driver": "com.simba.athena.jdbc.Driver",
        "url": "jdbc:awsathena://athena.eu-west-1.amazonaws.com:443",
        # "AwsCredentialsProviderClass": "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "User": access_key,
        "Password": secret_key,
        "S3OutputLocation": f"{s3_output_location}",
        "UseResultsetStreaming": 0,
        "query": read_animal_event_from_bronze,
    }
    hash_cols: List[str] = [
        "DAYS_animals_events_COOKED",
        "TOTAL_MEAL_DURATION",
        "TOTAL_GAS_USED",
        "LAST_MEAL_DATE",
    ]
    source_config = {
        "source_type": "athena",
        "format": "jdbc",
        "options": options,
        "hash_cols": hash_cols,
    }
    target_config = {
        "format": "delta",
        "language": "sql",
        "sql": write_animal_event_to_silver,
        "view_name": "animal_view",
    }

    animals_events_pipeline = AnimalsEvents(
        pipeline_name="load_animals_events_pipelines",
        source_config=source_config,
        target_config=target_config,
    )
    animals_events_pipeline.run()


spark_extra_config = [
    (
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.585,io.delta:delta-spark_2.12:3.0.0",
    ),
    (
        "spark.jars",
        "https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC-2.0.33.1003/AthenaJDBC42-2.0.33.jar",
    ),
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    (
        "log4j.logger.shutdownhookmanager.name",
        "org.apache.spark.util.ShutdownHookManager",
    ),
    ("log4j.logger.shutdownhookmanager.level", "OFF"),
    ("log4j.logger.sparkenv.name", "org.apache.spark.SparkEnv"),
    ("log4j.logger.sparkenv.level", "ERROR"),
    (
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    ),
    ("spark.local.‌​dir", "/temp/spark"),
    # ("spark.hadoop.io.native.lib", "false")
]
