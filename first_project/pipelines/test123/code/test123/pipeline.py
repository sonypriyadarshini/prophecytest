from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from test123.config.ConfigStore import *
from test123.udfs.UDFs import *
from prophecy.utils import *
from test123.graph import *

def pipeline(spark: SparkSession) -> None:
    df_csv = csv(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/test123")
    
    MetricsCollector.start(spark = spark, pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/test123")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
