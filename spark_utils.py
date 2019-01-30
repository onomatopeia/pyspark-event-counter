from pyspark.sql import SparkSession
from contextlib import contextmanager


@contextmanager
def spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
