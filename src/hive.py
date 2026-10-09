# -*- coding: utf-8 -*-
"""
Xây dựng cơ sở dữ liệu & Phân tích bằng HiveQL
Đăng ký cơ sở dữ liệu và bảng Hive trỏ vào HDFS Parquet
Phân tích tương tác, thời gian, top user, LAG window và xuất user_features.csv
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    DATA_CLEAN_DIR, HDFS_CLEAN_DIR, USER_FEATURES_CSV, OUTPUT_DIR,
    print_banner, get_spark_session
)

USER_FEATURE_COLS = [
    'user_id', 'total_interactions', 'retweet_count', 'mention_count',
    'reply_count', 'unique_targets', 'active_days', 'active_hours'
]

def run_hive(spark=None, clean_path=None):
    print_banner("XÂY DỰNG DỮ LIỆU VÀ PHÂN TÍCH BẰNG HIVEQL")

    stop_spark = False
    if spark is None:
        spark = get_spark_session("Higgs-Hive-Analysis")
        stop_spark = True

    if clean_path is None:
        try:
            spark.read.parquet(HDFS_CLEAN_DIR).take(1)
            clean_path = HDFS_CLEAN_DIR
        except Exception:
            clean_path = str(DATA_CLEAN_DIR).replace("\\", "/")

    # [3.5] Tạo Database và Bảng Hive trỏ tới Parquet
    print(f"-> Tạo Database 'higgs' và Table 'activity' trỏ tới: {clean_path}")
    spark.sql("CREATE DATABASE IF NOT EXISTS higgs COMMENT 'Phan tich HIGGS Twitter'")
    spark.sql("USE higgs")
    spark.sql("DROP TABLE IF EXISTS activity")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS activity
        USING PARQUET
        LOCATION '{clean_path}'
    """)
    spark.sql("MSCK REPAIR TABLE activity")

    preview_df = spark.sql("SELECT * FROM activity LIMIT 5")
    preview_df.show(5, truncate=False)
    total_hive_rows = spark.sql("SELECT COUNT(*) AS total FROM activity").collect()[0]["total"]
    print(f"=> Tổng số dòng trong bảng Hive 'activity': {total_hive_rows:,}")

    # [3.6.1] Thống kê tương tác
    print("\n--- Tổng quan người dùng & tương tác ---")
    spark.sql('''
        SELECT COUNT(*)               AS tong_tuong_tac,
               COUNT(DISTINCT user_a) AS so_user_thuc_hien,
               COUNT(DISTINCT user_b) AS so_user_duoc_tuong_tac
        FROM activity
    ''').show()

    print("--- Tỷ lệ phân bố các loại tương tác ---")
    df_type = spark.sql('''
        SELECT type_name,
               COUNT(*) AS so_luong,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS ty_le_phan_tram
        FROM activity
        GROUP BY type_name
        ORDER BY so_luong DESC
    ''')
    df_type.show()

    # [3.6.2] Theo thời gian
    print("--- Tương tác theo từng ngày ---")
    df_daily = spark.sql('''
        SELECT dt,
               SUM(CASE WHEN `type` = 'RT' THEN 1 ELSE 0 END) AS retweet,
               SUM(CASE WHEN `type` = 'MT' THEN 1 ELSE 0 END) AS mention,
               SUM(CASE WHEN `type` = 'RE' THEN 1 ELSE 0 END) AS reply,
               COUNT(*)                                       AS tong
        FROM activity
        GROUP BY dt
        ORDER BY dt
    ''')
    df_daily.show(15)

    print("---Tương tác theo khung giờ trong ngày ---")
    df_hourly = spark.sql('''
        SELECT `hour`,
               SUM(CASE WHEN `type` = 'RT' THEN 1 ELSE 0 END) AS retweet,
               SUM(CASE WHEN `type` = 'MT' THEN 1 ELSE 0 END) AS mention,
               SUM(CASE WHEN `type` = 'RE' THEN 1 ELSE 0 END) AS reply,
               COUNT(*)                                       AS tong
        FROM activity
        GROUP BY `hour`
        ORDER BY `hour`
    ''')
    df_hourly.show(24)

    # [3.6.3] Top Users
    print("--- Top 5 user được Retweet nhiều nhất ---")
    spark.sql('''
        SELECT user_b AS user_id, COUNT(*) AS so_lan_duoc_retweet
        FROM activity WHERE `type` = 'RT'
        GROUP BY user_b ORDER BY so_lan_duoc_retweet DESC LIMIT 5
    ''').show()

    print("--- Top 5 user hoạt động tích cực nhất ---")
    spark.sql('''
        SELECT user_a AS user_id, COUNT(*) AS tong_hanh_dong
        FROM activity GROUP BY user_a ORDER BY tong_hanh_dong DESC LIMIT 5
    ''').show()

    # [3.6.3.e] Tạo bảng user_features cho K-Means
    print("--- Tạo bảng đặc trưng 'user_features' ---")
    spark.sql("DROP TABLE IF EXISTS user_features")
    spark.sql("""
        CREATE TABLE user_features
        USING PARQUET
        AS
        SELECT
            user_a AS user_id,
            COUNT(*) AS total_interactions,
            SUM(CASE WHEN `type` = 'RT' THEN 1 ELSE 0 END) AS retweet_count,
            SUM(CASE WHEN `type` = 'MT' THEN 1 ELSE 0 END) AS mention_count,
            SUM(CASE WHEN `type` = 'RE' THEN 1 ELSE 0 END) AS reply_count,
            COUNT(DISTINCT user_b) AS unique_targets,
            COUNT(DISTINCT dt) AS active_days,
            COUNT(DISTINCT `hour`) AS active_hours
        FROM activity
        GROUP BY user_a
    """)

    # [3.6.3.f] Export user_features.csv
    print(f"--- Xuất bảng user_features ra: {USER_FEATURES_CSV} ---")
    user_features_df = spark.table('user_features').select(*USER_FEATURE_COLS)
    user_features_pd = user_features_df.toPandas()
    user_features_pd.to_csv(USER_FEATURES_CSV, index=False)
    print(f"=> Đã xuất thành công {len(user_features_pd):,} dòng đặc trưng người dùng.")

    # [3.6.4] Phân tích tăng trưởng & Sự kiện Higgs
    print("--- Tốc độ tăng trưởng ngày (Window LAG) ---")
    spark.sql('''
        SELECT dt, tong, tong_ngay_truoc,
               tong - tong_ngay_truoc AS chenh_lech,
               ROUND(100.0 * (tong - tong_ngay_truoc) / tong_ngay_truoc, 2) AS tang_truong_pct
        FROM (
            SELECT dt, tong, LAG(tong) OVER (ORDER BY dt) AS tong_ngay_truoc
            FROM (SELECT dt, COUNT(*) AS tong FROM activity GROUP BY dt) d
        ) x ORDER BY dt
    ''').show()

    print("--- So sánh giai đoạn Trước / Trong / Sau sự kiện Higgs ---")
    spark.sql('''
        SELECT period,
               COUNT(*) AS tong_tuong_tac,
               COUNT(DISTINCT dt) AS so_ngay,
               ROUND(COUNT(*) / COUNT(DISTINCT dt), 0) AS trung_binh_ngay
        FROM (
            SELECT dt,
                   CASE WHEN dt <  '2012-07-04' THEN '1_Truoc_su_kien'
                        WHEN dt =  '2012-07-04' THEN '2_Ngay_cong_bo_04_07'
                        ELSE '3_Sau_su_kien' END AS period
            FROM activity
        ) p GROUP BY period ORDER BY period
    ''').show()

    pdf_type = df_type.toPandas()
    pdf_daily = df_daily.toPandas()
    pdf_hourly = df_hourly.toPandas()

    pdf_type.to_csv(OUTPUT_DIR / "stats_type.csv", index=False)
    pdf_daily.to_csv(OUTPUT_DIR / "stats_daily.csv", index=False)
    pdf_hourly.to_csv(OUTPUT_DIR / "stats_hourly.csv", index=False)

    if stop_spark:
        spark.stop()

    return {
        "total_rows": total_hive_rows,
        "total_users": len(user_features_pd),
        "pdf_type": pdf_type,
        "pdf_daily": pdf_daily,
        "pdf_hourly": pdf_hourly
    }

if __name__ == "__main__":
    run_hive()
