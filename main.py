# -*- coding: utf-8 -*-
"""
"""

import sys
import time
import argparse
from pathlib import Path

# Đăng ký thư mục src vào sys.path
SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import (
    get_spark_session, print_banner, SUMMARY_REPORT_MD,
    CHARTS_DIR, USER_FEATURES_CSV
)
from hdfs import run_hdfs
from preprocess import run_preprocess
from hive import run_hive
from visualize import run_visualize
from kmeans import run_kmeans

def parse_args():
    parser = argparse.ArgumentParser(description="HIGGS Twitter Analysis Orchestrator")
    parser.add_argument(
        "--step",
        choices=["all", "hdfs", "preprocess", "hive", "viz", "kmeans"],
        default="all",
        help="Chọn bước cần chạy (Mặc định: 'all' - chạy toàn bộ pipeline)"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    start_total_time = time.time()

    print_banner("BẮT ĐẦU PIPELINE PHÂN TÍCH HIGGS TWITTER BIG DATA")
    print(f"=> Chế độ chạy: {args.step.upper()}")

    spark = None
    hdfs_info = {}
    prep_info = {}
    hive_info = {}
    kmeans_info = {}

    try:
        # [1] HDFS SETUP
        if args.step in ["all", "hdfs"]:
            hdfs_info = run_hdfs()

        if args.step == "hdfs":
            return

        # Khởi tạo SparkSession tinh gọn cho bước Big Data
        if args.step in ["all", "preprocess", "hive"]:
            spark = get_spark_session("Higgs-Analysis-Master")

        # [2] SPARK PREPROCESS
        if args.step in ["all", "preprocess"]:
            raw_path = hdfs_info.get("raw_path", None)
            prep_info = run_preprocess(spark=spark, input_raw_path=raw_path)

        if args.step == "preprocess":
            return

        # [3] HIVE & HIVEQL ANALYSIS
        if args.step in ["all", "hive"]:
            clean_path = prep_info.get("clean_path", None)
            hive_info = run_hive(spark=spark, clean_path=clean_path)

        # [4] VISUALIZATION (BIỂU ĐỒ)
        if args.step in ["all", "viz"]:
            pdf_type = hive_info.get("pdf_type", None)
            pdf_daily = hive_info.get("pdf_daily", None)
            pdf_hourly = hive_info.get("pdf_hourly", None)
            run_visualize(pdf_type, pdf_daily, pdf_hourly)

        if args.step in ["hive", "viz"]:
            return

    finally:
        # Giải phóng SparkSession trước khi chạy K-Means để hoàn trả RAM
        if spark is not None:
            spark.stop()
            print("\n=> [GIẢI PHÓNG BỘ NHỚ] Đã đóng SparkSession, hoàn trả RAM hoàn toàn cho OS.")

    # [5] K-MEANS MACHINE LEARNING
    if args.step in ["all", "kmeans"]:
        kmeans_info = run_kmeans(csv_path=USER_FEATURES_CSV)

    # [6] XUẤT BÁO CÁO TỔNG KẾT MARKDOWN
    total_time = time.time() - start_total_time
    if args.step == "all":
        with open(SUMMARY_REPORT_MD, "w", encoding="utf-8") as f:
            f.write("# BÁO CÁO KẾT QUẢ THỰC NGHIỆM HIGGS TWITTER ANALYSIS\n\n")
            f.write(f"- **Tổng thời gian thực thi**: {total_time:.2f} giây (~{total_time/60:.1f} phút)\n")
            if prep_info:
                f.write(f"- **Số bản ghi dữ liệu thô (Raw)**: {prep_info.get('n_raw', 0):,} dòng\n")
                f.write(f"- **Số bản ghi sạch sau tiền xử lý (Clean)**: {prep_info.get('n_clean', 0):,} dòng\n")
                f.write(f"- **Số dòng rác/lỗi đã loại bỏ**: {prep_info.get('removed_count', 0):,} dòng\n")
            if hive_info:
                f.write(f"- **Tổng số người dùng phân tích (User Features)**: {hive_info.get('total_users', 0):,} users\n")
            if kmeans_info:
                f.write(f"- **Thuật toán phân cụm**: K-Means ({kmeans_info.get('version')}, K={kmeans_info.get('best_k')})\n")
                f.write(f"- **Silhouette Score**: {kmeans_info.get('silhouette', 0):.4f}\n")
                f.write(f"- **Calinski-Harabasz Score**: {kmeans_info.get('calinski_harabasz', 0):.2f}\n")
                f.write(f"- **Davies-Bouldin Score**: {kmeans_info.get('davies_bouldin', 0):.4f}\n\n")
                f.write("## 1. Phân bố quy mô các cụm\n\n")
                f.write(kmeans_info.get('cluster_size').to_markdown() + "\n\n")
                f.write("## 2. Hồ sơ đặc trưng trung bình theo cụm\n\n")
                f.write(kmeans_info.get('cluster_profile').to_markdown() + "\n\n")
            f.write("## 3. Danh sách biểu đồ đã xuất bản\n\n")
            for img in sorted(CHARTS_DIR.glob("*.png")):
                f.write(f"- `charts/{img.name}`\n")

        print(f"\n=> [BÁO CÁO TỔNG HỢP] Đã tạo thành công: {SUMMARY_REPORT_MD.name}")

    print_banner(f"HOÀN THÀNH TẤT CẢ CÁC BƯỚC TRONG {total_time:.2f} GIÂY!")

if __name__ == "__main__":
    main()
