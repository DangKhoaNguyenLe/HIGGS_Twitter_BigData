# -*- coding: utf-8 -*-
"""
Bước 2: Tiền xử lý dữ liệu bằng PySpark
Đọc dữ liệu, làm sạch, chuẩn hóa thời gian và lưu Parquet phân vùng theo ngày (dt) vào HDFS
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    DATA_CLEAN_DIR, HDFS_CLEAN_DIR, LOCAL_DATASET_PATH,
    HDFS_RAW_FILE, print_banner, get_spark_session
)
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType

VALID_TYPES = ["RT", "MT", "RE"]

def run_preprocess(spark=None, input_raw_path=None, output_clean_path=None):
    """Tiền xử lý dữ liệu bằng PySpark."""
    print_banner("TIỀN XỬ LÝ DỮ LIỆU BẰNG PYSPARK")

    stop_spark = False
    if spark is None:
        spark = get_spark_session("Higgs-Spark-Preprocess")
        stop_spark = True

    if input_raw_path is None:
        try:
            spark.read.text(HDFS_RAW_FILE).take(1)
            input_raw_path = HDFS_RAW_FILE
            output_clean_path = HDFS_CLEAN_DIR
        except Exception:
            input_raw_path = str(LOCAL_DATASET_PATH).replace("\\", "/")
            output_clean_path = str(DATA_CLEAN_DIR).replace("\\", "/")
    elif output_clean_path is None:
        output_clean_path = HDFS_CLEAN_DIR if "hdfs://" in input_raw_path else str(DATA_CLEAN_DIR).replace("\\", "/")

    print(f"-> Nguồn đọc (Raw) : {input_raw_path}")
    print(f"-> Đích ghi (Clean): {output_clean_path}")

    # [3.4.1] Đọc dữ liệu
    schema = StructType([
        StructField("user_a", LongType()),
        StructField("user_b", LongType()),
        StructField("ts", LongType()),
        StructField("type", StringType()),
    ])

    raw = spark.read.csv(input_raw_path, sep=" ", schema=schema)
    n_raw = raw.count()
    print(f"1. Tổng số dòng đọc được từ raw: {n_raw:,}")

    # [3.4.2] Kiểm tra lỗi dữ liệu
    n_null_rows = raw.filter(
        F.col("user_a").isNull() | F.col("user_b").isNull() |
        F.col("ts").isNull() | F.col("type").isNull()
    ).count()

    n_invalid_type = raw.filter(
        F.col("type").isNotNull() & ~F.upper(F.trim("type")).isin(VALID_TYPES)
    ).count()

    n_bad_ts = raw.filter(F.col("ts") <= 0).count()
    n_duplicate = n_raw - raw.dropDuplicates().count()

    print(f"2. Thống kê lỗi:")
    print(f"   - Dòng chứa NULL             : {n_null_rows:,}")
    print(f"   - Dòng sai loại tương tác    : {n_invalid_type:,}")
    print(f"   - Dòng timestamp không hợp lệ: {n_bad_ts:,}")
    print(f"   - Dòng trùng lặp hoàn toàn   : {n_duplicate:,}")

    step = raw.dropna().filter(F.col("ts") > 0)

    # [3.4.3] Chuyển đổi Timestamp
    step = (
        step.withColumn("datetime", F.from_unixtime("ts", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("dt", F.from_unixtime("ts", "yyyy-MM-dd"))
            .withColumn("hour", F.hour(F.from_unixtime("ts")).cast("int"))
    )

    # [3.4.4] Chuẩn hóa tương tác & xóa trùng lặp
    clean = (
        step.withColumn("type", F.upper(F.trim("type")))
            .filter(F.col("type").isin(VALID_TYPES))
            .withColumn("type_name",
                        F.when(F.col("type") == "RT", "Retweet")
                         .when(F.col("type") == "MT", "Mention")
                         .otherwise("Reply"))
            .dropDuplicates(["user_a", "user_b", "ts", "type"])
            .select("user_a", "user_b", "ts", "type", "type_name", "datetime", "hour", "dt")
    )

    n_clean = clean.count()
    removed_count = n_raw - n_clean
    print(f"3. Kết quả làm sạch:")
    print(f"   - Số dòng ban đầu   : {n_raw:,}")
    print(f"   - Số dòng sau xử lý : {n_clean:,}")
    print(f"   - Đã loại bỏ        : {removed_count:,} dòng ({(removed_count/n_raw)*100:.2f}%)")

    # [3.4.5] Lưu Parquet phân vùng theo ngày (dt)
    print(f"4. Ghi dữ liệu Parquet phân vùng vào: {output_clean_path}")
    clean.write.mode("overwrite").partitionBy("dt").parquet(output_clean_path)
    print("=> Lưu Parquet sạch vào HDFS thành công!")

    # Đồng thời lưu một bản sao vào thư mục local data/clean để xem trực tiếp trên máy host
    local_clean_path = str(DATA_CLEAN_DIR).replace("\\", "/")
    if output_clean_path != local_clean_path:
        try:
            print(f"   -> Lưu bản sao Parquet vào thư mục local: {local_clean_path}")
            clean.write.mode("overwrite").partitionBy("dt").parquet(local_clean_path)
            print("   => Đã đồng bộ bản sao Parquet ra thư mục local data/clean thành công!")
        except Exception as e:
            print(f"   Không ghi bản sao local: {e}")

    if stop_spark:
        spark.stop()

    return {
        "n_raw": n_raw,
        "n_clean": n_clean,
        "removed_count": removed_count,
        "clean_path": output_clean_path
    }

if __name__ == "__main__":
    run_preprocess()
