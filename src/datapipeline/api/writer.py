from dataclasses import dataclass, field
from logging import error
from typing import Tuple
from datapipeline.utils.logger_config import Logger
from datapipeline.errors.base_error import PipelineError
from pyspark import sql
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime


@dataclass
class Writer(Logger):
    spark: SparkSession

    def write(
        self, target_config: dict, source_dataframe: DataFrame
    ) -> Tuple[DataFrame, bool]:
        try:
            if (
                target_config["format"] == "delta"
                and target_config["language"] == "sql"
            ):
                self.spark.sql("USE CATALOG ml_ai_catalog")
                self.spark.sql("use schema ml_ai_schema")
                source_dataframe.createOrReplaceTempView(target_config["view_name"])
                target_results = self.spark.sql(target_config["sql"])
                return target_results
        except Exception as error:
            raise PipelineError(f"Error writing dataframe {error}")


#         elif self.target_config['format'] == 'parquet':
#             return self.write_parquet()
#         else:
#             raise PipelineError('Invalid target format')
# '
