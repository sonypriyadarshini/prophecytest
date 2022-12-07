from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from git_pipeline.config.ConfigStore import *
from git_pipeline.udfs.UDFs import *

def csv1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv")
