from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from git_pipeline.config.ConfigStore import *
from git_pipeline.udfs.UDFs import *
from prophecy.utils import *
from git_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_csv1 = csv1(spark)
    df_Filter_1 = Filter_1(spark, df_csv1)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_Filter_1)
    df_Subgraph_1 = Subgraph_1(spark, df_RowDistributor_1_out0)
    df_RestAPIEnrich_1 = RestAPIEnrich_1(spark, df_RowDistributor_1_out1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/git_pipeline")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/git_pipeline"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
