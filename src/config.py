# -*- coding: utf-8 -*-


import os
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
BASE_DIR = SRC_DIR.parent

DATA_DIR = BASE_DIR / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_CLEAN_DIR = DATA_DIR / "clean"
OUTPUT_DIR = BASE_DIR / "output"
CHARTS_DIR = OUTPUT_DIR / "charts"

DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
DATA_CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

DATASET_NAME = "higgs-activity_time.txt"
LOCAL_DATASET_PATH = DATA_RAW_DIR / DATASET_NAME

# Cấu hình cụm HDFS trên Docker
HDFS_URL = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:8020")
HDFS_RAW_DIR = f"{HDFS_URL}/data/higgs/raw"
HDFS_RAW_FILE = f"{HDFS_RAW_DIR}/{DATASET_NAME}"
HDFS_CLEAN_DIR = f"{HDFS_URL}/data/higgs/clean"

# Tệp đầu ra kết quả
USER_FEATURES_CSV = OUTPUT_DIR / "user_features.csv"
USER_CLUSTERS_CSV = OUTPUT_DIR / "user_clusters_final.csv"
KMEANS_METRICS_CSV = OUTPUT_DIR / "kmeans_metrics_comparison.csv"
CLUSTER_PROFILE_CSV = OUTPUT_DIR / "cluster_profile_final.csv"
SUMMARY_REPORT_MD = OUTPUT_DIR / "summary_report.md"

def get_spark_session(app_name="Higgs-Analysis-8GB"):
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1536m")
        .config("spark.executor.memory", "1536m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def print_banner(title):
    print("\n" + "=" * 70)
    print(f" {title} ")
    print("=" * 70)
