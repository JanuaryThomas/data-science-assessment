from boto3.dynamodb.conditions import Key
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dataclasses import dataclass, field
import time
import boto3
from abc import ABC, abstractmethod
from queue import Queue
from delta.tables import *
import json
import decimal
from pyspark.sql.types import StructType
from datapipeline.utils.logger_config import Logger
from datapipeline.errors.base_error import PipelineError
from pyspark.sql import SparkSession
from dataclasses import dataclass
from pyspark import SparkConf

# dynamodb = boto3.resource("dynamodb")


# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, decimal.Decimal):
#             return float(o)
#         return super(DecimalEncoder, self).default(o)


@dataclass
class Pipeline(ABC, Logger):
    pipeline_name: str
    source_config: dict
    target_config: dict
    spark_extra_configs: dict
    # spark = SparkSession.builder.getOrCreate()

    def __post_init__(self):
        if self.spark_extra_configs:
            conf = SparkConf().setAll(self.spark_extra_configs)
            self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        else:
            self.spark = SparkSession.builder.getOrCreate()

    def __add_pipeline_checkpoint_to_s3(
        self, item: DataFrame, format: str = "delta"
    ) -> Tuple[DataFrame, bool]:
        pass

    def __add_pipeline_checkpoint_to_dynamodb(self, item):
        pass
        # try:
        #     table: dynamodb.Table = dynamodb.Table(self.config_table_name)
        #     table.put_item(Item={})
        # except Exception as error:
        #     raise PipelineError from error

    def __split_s3_path(self, path: str):
        path_parts = path.replace("s3://", "").split("/")
        bucket = path_parts.pop(0)
        key = "/".join(path_parts)
        return bucket, key

    def __get_s3_files__(self, bucket: str, prefix: str):
        s3_client = boto3.client("s3")
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [f"s3a://{bucket}/{obj['Key']}" for obj in response["Contents"]]
        except Exception as error:
            raise PipelineError(f"Error occurred, {error}")

    def __pipeline_config_from_dynamodb(self, partition_key: str, limit=1) -> list:
        pass
        # table = dynamodb.Table(self.config_table_name)
        # try:
        #     results = table.query(
        #         Limit=limit,
        #         ScanIndexForward=False,
        #         KeyConditionExpression=Key("partitionKey").eq(partition_key)
        #         & Key("sortKey").lte(int(time.time())),
        #     )
        #     return results["Items"]
        # except Exception as error:
        #     raise PipelineError from error

    @abstractmethod
    def __commit__(self):
        pass

    @abstractmethod
    def extract(self, header: bool = False) -> DataFrame:
        pass

    def transform(self, rules: list):
        self.data = self.apply_rules(self.data, rules)

    @abstractmethod
    def load(self) -> DataFrame:
        pass

    # @abstractmethod
    # def apply_rules(self, df: DataFrame, rules: list) -> DataFrame:
    #     pass

    # def __merge__(
    #     self, output_path: str, format: str = "delta", coalesce=2, mode="overwrite"
    # ):
    #     if format != "delta":
    #         raise NotImplementedError("You need to need to enable delta lake support")
    #     if self.data.count() > 0 and DeltaTable.isDeltaTable(self.spark, output_path):
    #         current_delta_table = DeltaTable.forPath(self.spark, output_path)

    #         source_cols = self.data.columns
    #         source_dtypes = self.data.dtypes

    #         target_cols = current_delta_table.toDF().columns
    #         target_dtypes = current_delta_table.toDF().dtypes

    #         conditions = []

    #         # # Iterate through the column names and data types of both tables
    #         for i in range(len(source_cols)):
    #             if (
    #                 source_cols[i] in target_cols
    #                 and source_dtypes[i][1] == target_dtypes[i][1]
    #             ):
    #                 conditions.append(
    #                     f"temp_table.{source_cols[i]} = current_table.{source_cols[i]}"
    #                 )

    #         # Generate the condition string
    #         condition = " and ".join(conditions)

    #         current_delta_table.alias("current_table").merge(
    #             self.data.alias("temp_table"), condition
    #         ).whenNotMatchedInsertAll().execute()
    #         # current_delta_table.generate('symlink_format_manifest')
    #         deduplicated_df = (
    #             DeltaTable.forPath(self.spark, output_path).toDF().distinct()
    #         )
    #         deduplicated_df.coalesce(coalesce).write.format("delta").mode(
    #             "overwrite"
    #         ).save(output_path)
    #         DeltaTable.forPath(self.spark, output_path).generate(
    #             "symlink_format_manifest"
    #         )
    #         self._is_loaded = True
    #     elif self.data.count() > 0 and not DeltaTable.isDeltaTable(
    #         self.spark, output_path
    #     ):
    #         self.data.coalesce(coalesce).write.format(format).mode(mode).save(
    #             output_path
    #         )
    #         DeltaTable.forPath(self.spark, output_path).generate(
    #             "symlink_format_manifest"
    #         )
    #         self._is_loaded = True
    #     else:
    #         self._is_loaded = False

    # def load(
    #     self,
    #     output_path: str,
    #     partitions_key: list = None,
    #     format="delta",
    #     coalesce=1,
    #     mode="append",
    # ):
    #     self.__merge__(output_path=output_path, format=format, coalesce=coalesce)

    #     if self._is_loaded:
    #         self.__commit__()

    # def __apply_mapping__(self, df_schema: list, source_schema: list):
    #     mapped_columns = [
    #         (point[0], _point[1])
    #         for point in df_schema
    #         for _point in source_schema
    #         if _point[0] == point[0]
    #     ]
    #     return [F.col(col).cast(type_) for col, type_ in mapped_columns]

    # def apply_mapping(self, schema):
    #     mapping_columns: list = self.__apply_mapping__(self.data.dtypes, schema)
    #     self.data = self.data.select(*mapping_columns)

    # def __commit__(self):
    #     status = "COMPLETED"  # COMPLETED_WITH_ERRORS

    #     item = {
    #         "partitionKey": self.pipeline_name,
    #         "sortKey": int(time.time()),
    #         "files": ",".join(self._processed_files),
    #         "status": status,
    #         "batch_id": self._batch_id,
    #         "rows_sourced": self.data.count(),
    #         "errors": "",
    #     }

    #     try:
    #         table = dynamodb.Table(self.config_table_name)
    #         table.put_item(Item=item)
    #     except Exception as error:
    #         raise PipelineError from error

    # def __clear_checkpoint__(self, partition_key: str, sort_key: int):
    #     table = dynamodb.Table(self.config_table_name)
    #     try:
    #         table.delete_item(Key={"partitionKey": partition_key, "sortKey": sort_key})
    #     except Exception as error:
    #         raise PipelineError from error

    # def __store_execptions__(self, row):
    #     # read from sqs
    #     exception_df = ""
