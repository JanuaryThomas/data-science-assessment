from dataclasses import dataclass, field
from typing import Tuple
from datapipeline.utils.logger_config import Logger
from datapipeline.errors.base_error import PipelineError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime


@dataclass
class Read(Logger):
    source_config: dict
    spark: SparkSession

    read_stats: DataFrame = None

    def read(self) -> Tuple[DataFrame, bool]:
        try:
            source_dataframe: DataFrame = (
                self.spark.read.format(self.source_config["format"])
                .options(**self.source_config["options"])
                .load()
            )
            if self.source_config["hash_cols"]:
                source_dataframe = source_dataframe.withColumn(
                    "HASH",
                    F.lit(
                        F.sha2(F.concat_ws("~", *self.source_config["hash_cols"]), 256)
                    ),
                )

            return source_dataframe, True

        except Exception as e:
            self.logger.error(f"Error reading from source: {e}")
            raise PipelineError(f"Error reading from source: {e}")
