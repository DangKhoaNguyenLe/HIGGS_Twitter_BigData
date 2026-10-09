# -*- coding: utf-8 -*-
"""
Bước 1: Thiết lập & Nạp dữ liệu vào cụm Hadoop HDFS
Lưu trữ dữ liệu trên HDFS (raw & clean)
"""

import sys
import shutil
import urllib.request
import gzip
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    LOCAL_DATASET_PATH, DATA_RAW_DIR, BASE_DIR,
    HDFS_URL, HDFS_RAW_FILE, print_banner, get_spark_session
)

FALLBACK_PATHS = [
    LOCAL_DATASET_PATH,
    BASE_DIR.parent / "higgs-activity_time.txt",
    Path("/project/higgs-activity_time.txt"),
    Path("/app/data/raw/higgs-activity_time.txt")
]

def ensure_local_dataset():

    for p in FALLBACK_PATHS:
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            if p != LOCAL_DATASET_PATH:
                shutil.copyfile(p, LOCAL_DATASET_PATH)
            return LOCAL_DATASET_PATH

    print("=> Tải dataset từ Stanford SNAP...")
    url = "https://snap.stanford.edu/data/higgs-activity_time.txt.gz"
    gz_path = DATA_RAW_DIR / "higgs-activity_time.txt.gz"
    urllib.request.urlretrieve(url, gz_path)
    with gzip.open(gz_path, "rb") as f_in, open(LOCAL_DATASET_PATH, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    gz_path.unlink()
    return LOCAL_DATASET_PATH

def run_hdfs(spark=None):
    """Thiết lập lưu trữ HDFS."""
    print_banner("THIẾT LẬP LƯU TRỮ TRÊN CỤM HADOOP HDFS (RAW & CLEAN)")
    local_file = ensure_local_dataset()
    file_size_mb = local_file.stat().st_size / (1024 * 1024)
    print(f"Dataset thô: {local_file.name} | Dung lượng: {file_size_mb:.2f} MB")

    with open(local_file, "r", encoding="utf-8") as f:
        preview = [next(f) for _ in range(5)]
    print("Mẫu 5 dòng đầu tiên:")
    for line in preview:
        print("  ", line.strip())

    stop_spark = False
    if spark is None:
        spark = get_spark_session("Higgs-HDFS-Setup")
        stop_spark = True

    use_hdfs = False
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        uri = sc._gateway.jvm.java.net.URI(HDFS_URL)
        fs = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)

        raw_dir = sc._gateway.jvm.org.apache.hadoop.fs.Path("/data/higgs/raw")
        clean_dir = sc._gateway.jvm.org.apache.hadoop.fs.Path("/data/higgs/clean")
        target_file = sc._gateway.jvm.org.apache.hadoop.fs.Path("/data/higgs/raw/higgs-activity_time.txt")

        if not fs.exists(raw_dir):
            fs.mkdirs(raw_dir)
        if not fs.exists(clean_dir):
            fs.mkdirs(clean_dir)

        if not fs.exists(target_file):
            print(f"=> [HDFS] Nạp tệp lên HDFS: /data/higgs/raw/ ...")
            local_src = sc._gateway.jvm.org.apache.hadoop.fs.Path(str(local_file).replace("\\", "/"))
            fs.copyFromLocalFile(False, True, local_src, target_file)
            print("=> [HDFS] Nạp dữ liệu lên NameNode thành công!")
        else:
            print("=> [HDFS] Tệp dữ liệu đã sẵn sàng trên HDFS: /data/higgs/raw/higgs-activity_time.txt")

        use_hdfs = True
        print(f"=> [HDFS STATUS] NameNode hoạt động tại {HDFS_URL}")
        print("=> [HDFS WEB UI] Theo dõi tại: http://localhost:9870")
    except Exception as e:
        print(f"=> [CHÚ Ý] Không kết nối trực tiếp được NameNode ({e}). Sẽ sử dụng volume lưu trữ cục bộ.")
        use_hdfs = False

    if stop_spark:
        spark.stop()

    return {
        "use_hdfs": use_hdfs,
        "raw_path": HDFS_RAW_FILE if use_hdfs else str(local_file).replace("\\", "/"),
        "file_size_mb": file_size_mb
    }

if __name__ == "__main__":
    run_hdfs()
