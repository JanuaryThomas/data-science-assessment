from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, LongType, TimestampType
import re
import time
from multiprocessing import Queue, Manager, Lock

errors = Manager().Queue()


@udf(LongType())
def time_offset(epoch: int, row) -> int:
    now = time.mktime(time.gmtime())
    try:
        if int(epoch) > now:
            return int(epoch / 1000)
    except Exception as _:
        # TODO: store data with S3 -> awsdatawrangler
        return
    return int(epoch)


@udf(TimestampType())
def timestamp_to_iso(epoch: int, row):
    try:
        if epoch is not None:
            return datetime.utcfromtimestamp(int(epoch))
    except Exception as error:
        print(error)
