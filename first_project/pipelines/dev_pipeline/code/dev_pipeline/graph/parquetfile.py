from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dev_pipeline.config.ConfigStore import *
from dev_pipeline.udfs.UDFs import *

def parquetfile(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("parquet")\
        .load("dbfs:/databricks-datasets/amazon/test4K/part-r-00000-64a9bd4a-25fc-48e6-8a60-2fd057bddd27.gz.parquet")
