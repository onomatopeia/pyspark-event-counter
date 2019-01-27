import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
import os
import time


LOCATION_CSV = 'location.csv'
REGION_CSV = 'region.csv'
RESULT_CSV = 'result.csv'


# TODO: validate schema on csv files?

def process(spark, dir):
    df = spark.read.csv(os.path.join(dir, LOCATION_CSV), header=True)
    regions = spark.read.csv(os.path.join(dir, REGION_CSV), header=True)

    df.withColumn('event_count', fn.lit(1))\
        .groupBy(['region_id', 'date'])\
        .agg(fn.sum(fn.col('event_count')).alias('count'))\
        .join(regions, df.region_id == regions.region_id)\
        .select('region_name', 'date', 'count')\
        .repartition(1)\
        .write.csv(path=os.path.join(dir, RESULT_CSV), quoteAll=True, mode='overwrite', header=True)


if __name__ == '__main__':
    findspark.init()
    spark_session = SparkSession.builder.getOrCreate()
    directory = os.path.join('d:/', 'python', 'qrious', 'test', 'data')
    t0 = time.time()
    process(spark_session, directory)
    t1 = time.time() - t0
    spark_session.stop()
    print(f'Processing took {t1} seconds.')


