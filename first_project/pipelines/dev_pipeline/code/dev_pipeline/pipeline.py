from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dev_pipeline.config.ConfigStore import *
from dev_pipeline.udfs.UDFs import *
from prophecy.utils import *
from dev_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_parquetfile = parquetfile(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/dev_pipeline")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/dev_pipeline"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
