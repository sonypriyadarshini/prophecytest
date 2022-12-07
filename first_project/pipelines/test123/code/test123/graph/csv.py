from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from test123.config.ConfigStore import *
from test123.udfs.UDFs import *

def csv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv")
