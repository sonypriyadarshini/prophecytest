from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from git_pipeline.config.ConfigStore import *
from git_pipeline.udfs.UDFs import *

def csv1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("EventId", StringType(), True), StructField("DER_mass_MMC", StringType(), True), StructField("DER_mass_transverse_met_lep", StringType(), True), StructField("DER_mass_vis", StringType(), True), StructField("DER_pt_h", StringType(), True), StructField("DER_deltaeta_jet_jet", StringType(), True), StructField("DER_mass_jet_jet", StringType(), True), StructField("DER_prodeta_jet_jet", StringType(), True), StructField("DER_deltar_tau_lep", StringType(), True), StructField("DER_pt_tot", StringType(), True), StructField("DER_sum_pt", StringType(), True), StructField("DER_pt_ratio_lep_tau", StringType(), True), StructField("DER_met_phi_centrality", StringType(), True), StructField("DER_lep_eta_centrality", StringType(), True), StructField("PRI_tau_pt", StringType(), True), StructField("PRI_tau_eta", StringType(), True), StructField("PRI_tau_phi", StringType(), True), StructField("PRI_lep_pt", StringType(), True), StructField("PRI_lep_eta", StringType(), True), StructField("PRI_lep_phi", StringType(), True), StructField("PRI_met", StringType(), True), StructField("PRI_met_phi", StringType(), True), StructField("PRI_met_sumet", StringType(), True), StructField("PRI_jet_num", StringType(), True), StructField("PRI_jet_leading_pt", StringType(), True), StructField("PRI_jet_leading_eta", StringType(), True), StructField("PRI_jet_leading_phi", StringType(), True), StructField("PRI_jet_subleading_pt", StringType(), True), StructField("PRI_jet_subleading_eta", StringType(), True), StructField("PRI_jet_subleading_phi", StringType(), True), StructField("PRI_jet_all_pt", StringType(), True), StructField("Weight", StringType(), True), StructField("Label", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv")
