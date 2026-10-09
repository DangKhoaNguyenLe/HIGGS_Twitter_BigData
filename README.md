# HIGGS Twitter Big Data Analysis 
## 1. Cấu trúc dự án (Đơn giản & Không thư mục con lồng nhau)

```text
higgs_docker/
│
├── main.py                 # [CHÍNH] Điều phối chạy đồng loạt toàn bộ pipeline
├── Dockerfile              # Môi trường PySpark 3.5 + Scikit-Learn + Seaborn
├── docker-compose.yml      # Quản lý cụm HDFS (NameNode, DataNode) và higgs-analysis
├── hadoop.env              # Biến môi trường cấu hình cụm Hadoop HDFS
├── requirements.txt        # Danh sách thư viện Python phục vụ Big Data & ML
│
├── data/
│   ├── raw/                # Chứa dataset gốc 
│   └── clean/              # Dữ liệu Parquet sạch phân vùng theo ngày (dt)
│
├── output/
│   ├── summary_report.md   # Báo cáo tổng kết toàn bộ pipeline
│   ├── user_features.csv   # 7 đặc trưng người dùng sinh ra từ HiveQL
│   ├── user_clusters_final.csv # Kết quả phân cụm người dùng sau K-Means
│   ├── cluster_profile_final.csv # Hồ sơ đặc trưng trung bình theo cụm
│   ├── kmeans_metrics_comparison.csv # So sánh Inertia, Silhouette, CH, DB
│   └── charts/             # Toàn bộ 9 biểu đồ PNG 300 DPI xuất tự động
│
└── src/
    ├── config.py           # Thiết lập đường dẫn, hằng số & SparkSession chung
    ├── hdfs.py             # Nạp dữ liệu vào cụm Hadoop HDFS (raw & clean)
    ├── preprocess.py       # Tiền xử lý & lưu Parquet phân vùng vào HDFS
    ├── hive_analysis.hql   # Kịch bản HiveQL thuần chuẩn Apache Hive
    ├── hive.py             # Điều phối HiveQL, Window LAG & tạo user_features.csv
    ├── visualize.py        # Biểu đồ tròn, biểu đồ ngày & cột giờ
    └── kmeans.py           # K-Means Baseline vs Log1p, PCA 2D, hồ sơ cụm
├── run_hive_cli.bat        # Phím tắt mở terminal Apache Hive CLI (hive>)
├── run_hive_script.bat     # Phím tắt chạy kịch bản hive_analysis.hql trên Hive Server
```

---

## 2. Công nghệ sử dụng (Technology Stack & Architecture)


### 2.1. Bảng tổng hợp công nghệ

| Thành phần | Công nghệ / Thư viện | Phiên bản | Vai trò & Mục đích sử dụng |
| :--- | :--- | :--- | :--- |
| **Lưu trữ phân tán (HDFS)** | **Apache Hadoop HDFS** | `3.2.1` | Hệ thống tệp phân tán lưu trữ toàn bộ dữ liệu thô (raw) và dữ liệu đã làm sạch (clean) với kiến trúc Master-Worker (`NameNode` và `DataNode`). |
| **Định dạng dữ liệu** | **Apache Parquet** | `Snappy` | Lưu trữ dạng cột (Columnar format) phân vùng theo ngày (`dt`), tối ưu hóa I/O và tăng tốc truy vấn phân tích. |
| **Kho dữ liệu & Metastore** | **Apache Hive (Native)** | `2.3.2` | Gồm cụm 3 container: `PostgreSQL Metastore DB`, `Hive Metastore Service` (port 9083) và `HiveServer2` (port 10000). Hỗ trợ Hive CLI, Beeline và truy vấn trực tiếp Parquet trên HDFS. |
| **Ngôn ngữ truy vấn** | **HiveQL (`.hql`)** | `HQL 2.3+` | Kịch bản `src/hive_analysis.hql` định nghĩa DDL Database, Table phân vùng, MSCK Repair, hàm Window (`LAG`), CTAS tạo `user_features`. |
| **Xử lý phân tán** | **Apache Spark (PySpark)** | `3.5.0` | Tính toán in-memory phân tán, tiền xử lý, kiểm tra tính toàn vẹn, chuẩn hóa timestamp và ghi dữ liệu phân vùng vào HDFS. |
| **Học máy & Khai phá dữ liệu** | **Scikit-Learn** | `>=1.3.0` | Huấn luyện thuật toán phân cụm **K-Means**, chuẩn hóa `StandardScaler`, đánh giá $K \in [2, 8]$ (Elbow, Silhouette, Calinski-Harabasz, Davies-Bouldin), và giảm chiều dữ liệu bằng **PCA (2D)**. *(Tự động kích hoạt GPU cuML nếu có)*. |
| **Xử lý dữ liệu bảng** | **Pandas & PyArrow** | `Pandas >= 2.0`<br>`PyArrow >= 12.0` | Chuyển đổi zero-copy giữa Spark DataFrame và Python runtime, trích xuất dữ liệu ma trận đặc trưng người dùng và xuất file báo cáo. |
| **Trực quan hóa dữ liệu** | **Matplotlib & Seaborn** | `Matplotlib >= 3.7`<br>`Seaborn >= 0.12` | Vẽ tự động 9 biểu đồ trực quan hóa dữ liệu thống kê và mô hình phân cụm ở độ phân giải cao (300 DPI, backend `Agg`). |
| **Đóng gói & Ảo hóa** | **Docker & Docker Compose** | `Compose v2` | Cô lập môi trường chạy cụm Hadoop HDFS, cụm Apache Hive và ứng dụng Spark; quản lý RAM/CPU, biến môi trường và volume tự động. |
| **Môi trường chạy Java** | **OpenJDK (Java 8)** | `HotSpot 8u` | Runtime nền tảng cho Hadoop NameNode/DataNode, Hive Server và JVM của Apache Spark. Tối ưu bộ nhớ bằng G1GC (`-XX:+UseG1GC`). |


---

### 2.2. Chi tiết vai trò kỹ thuật từng thành phần trong Pipeline

#### 1. Tầng lưu trữ (Storage Layer) - Hadoop HDFS & Apache Parquet
- **Cụm HDFS (Hadoop 3.2.1)**:
  - **NameNode** (`higgs_namenode`, Port `9870` / `8020`): Quản trị namespace tệp phân tán, theo dõi ánh xạ block dữ liệu.
  - **DataNode** (`higgs_datanode`, Port `9864`): Lưu trữ vật lý các khối block dữ liệu với cơ chế đảm bảo an toàn.
  - Đường dẫn lưu trữ:
    - Raw: `hdfs://namenode:8020/data/higgs/raw/higgs-activity_time.txt` (~14 MB, 563.069 bản ghi).
    - Clean: `hdfs://namenode:8020/data/higgs/clean/` (lưu trữ theo định dạng Parquet phân vùng theo thư mục `dt=YYYY-MM-DD`).
- **Định dạng Parquet & Phân vùng (Partitioning)**:
  - Nén Snappy giúp giảm dung lượng lưu trữ trên đĩa.
  - Lưu trữ theo cột (Columnar) giúp Hive và Spark chỉ đọc đúng các cột cần thiết (Column Pruning) và bỏ qua các ngày không cần truy vấn (Partition Pruning).

#### 2. Tầng xử lý & Truy vấn (Processing & Query Layer) - PySpark & HiveQL
- **PySpark 3.5.0 (`preprocess.py`)**:
  - Đọc dữ liệu thô với schema định nghĩa trước (`user_a`, `user_b`, `ts`, `type`).
  - Kiểm tra giá trị Null/NaN, loại bỏ dữ liệu bất thường và lọc các kiểu tương tác hợp lệ (`RT` - Retweet, `MT` - Mention, `RE` - Reply).
  - Chuẩn hóa Unix timestamp sang định dạng thời gian chuẩn `yyyy-MM-dd HH:mm:ss`, sinh các cột thời gian phục vụ phân tích: `dt` (ngày), `hour` (giờ), `day_of_week`.
- **HiveQL / Spark SQL Engine (`hive.py`)**:
  - Khởi tạo Database `higgs` và liên kết bảng ngoài `activity` vào đường dẫn Parquet HDFS (`USING PARQUET LOCATION ...`).
  - Thực thi kỹ thuật `MSCK REPAIR TABLE activity` để đồng bộ metadata phân vùng tự động.
  - Phân tích thống kê: Tỷ lệ các loại tương tác, xu hướng biến động theo ngày, phân bố 24 khung giờ, Top 10 người dùng hoạt động tích cực nhất.
  - Sử dụng hàm nâng cao **Window Function (`LAG(ts) OVER (PARTITION BY user_a ORDER BY ts)`)** để tính toán thời gian phản hồi/giãn cách giữa các lượt tương tác của từng user.
  - Xây dựng bảng **7 đặc trưng người dùng** (`user_features.csv` gồm: `total_interactions`, `retweet_count`, `mention_count`, `reply_count`, `unique_targets`, `active_days`, `active_hours`) làm đầu vào cho thuật toán Machine Learning.

#### 3. Tầng Học máy (Machine Learning Layer) - K-Means, PCA & Metrics
- **Tiền xử lý & Chuẩn hóa**:
  - Sử dụng **`StandardScaler`** để chuẩn hóa dữ liệu 7 chiều về phân phối chuẩn (Zero Mean, Unit Variance).
  - Hỗ trợ thử nghiệm log-transform `log1p` để xử lý hiện tượng phân phối lệch đuôi dài (Power-law/Long-tail distribution) phổ biến trong dữ liệu mạng xã hội.
- **K-Means Clustering (`kmeans.py`)**:
  - Đánh giá tối ưu số cụm $K \in [2, 8]$ thông qua 4 chỉ số khoa học:
    - **Inertia (Elbow Method)**: Tổng bình phương khoảng cách tới tâm cụm.
    - **Silhouette Score**: Đo độ chặt chẽ trong cụm và phân tách giữa các cụm (áp dụng lấy mẫu phân tầng Stratified 20.000 điểm để tối ưu thời gian và RAM).
    - **Calinski-Harabasz Index**: Tỷ số giữa phương sai liên cụm và nội cụm.
    - **Davies-Bouldin Index**: Đánh giá mức độ tương đồng giữa các cụm.
  - Huấn luyện mô hình tối ưu tại $K = 4$, phân loại 279.379 người dùng vào 4 nhóm hành vi rõ rệt (Người dùng vãng lai, Người dùng tương tác trung bình, Người dùng tích cực, Người dùng siêu hoạt động / Key Opinion Leaders).
- **Giảm chiều dữ liệu PCA**:
  - Chiếu không gian 7 đặc trưng xuống **PCA 2D** (`PC1`, `PC2`) để trực quan hóa không gian phân tách cụm.




## 3. Quy trình chạy 

### Cách 1: 
Nhấp đúp chuột vào file [`run.bat`](run.bat).  
Hệ thống sẽ tự động:
1. Khởi động cụm HDFS (`higgs_namenode` và `higgs_datanode`).
2. Kích hoạt container chạy [`main.py`](main.py).
3. `main.py` sẽ tự động thực thi tuần tự các file:
   - **`hdfs.py`** ➔ Nạp dữ liệu vào HDFS `/data/higgs/raw/`.
   - **`preprocess.py`** ➔ Làm sạch dữ liệu, ghi Parquet phân vùng theo ngày vào HDFS `/data/higgs/clean/`.
   - **`hive.py`** ➔ Đăng ký bảng Hive `activity`, truy vấn HiveQL, tạo bảng và xuất `user_features.csv`.
   - **`visualize.py`** ➔ Vẽ và lưu 3 biểu đồ thống kê mạng xã hội ra `output/charts/`.
   - **`kmeans.py`** ➔ Đọc `user_features.csv`, đánh giá $K \in [2, 8]$, huấn luyện K-Means ($K=4$), vẽ biểu đồ PCA 2D và hồ sơ cụm, xuất toàn bộ bảng CSV phân cụm.
   - **Tổng kết** ➔ Tạo báo cáo tổng hợp Markdown [`output/summary_report.md`](output/summary_report.md).

---

### Cách 2: Chạy qua dòng lệnh Docker Compose
```powershell
cd c:\Users\nguye\Documents\CaNhan\BigData\CuoiKy\project\higgs_docker

# Chạy đồng loạt toàn bộ pipeline
docker compose run --rm higgs-analysis

# Hoặc chạy riêng từng file cụ thể nếu muốn:
docker compose run --rm higgs-analysis python /app/main.py --step hdfs
docker compose run --rm higgs-analysis python /app/main.py --step preprocess
docker compose run --rm higgs-analysis python /app/main.py --step hive
docker compose run --rm higgs-analysis python /app/main.py --step viz
docker compose run --rm higgs-analysis python /app/main.py --step kmeans
```

---

### Cách 3: Sử dụng Apache Hive Native (Hive CLI & HiveQL Script)
Bạn có thể thao tác và chạy truy vấn HiveQL trực tiếp trên container **`higgs_hive_server`**:
1. **Chạy toàn bộ kịch bản HiveQL (`src/hive_analysis.hql`)**:
   - Nhấp đúp vào file [`run_hive_script.bat`](run_hive_script.bat).
   - Hoặc chạy qua dòng lệnh:
     ```powershell
     docker compose exec hive-server hive -f /project/src/hive_analysis.hql
     ```
2. **Mở terminal Apache Hive CLI tương tác (`hive>`)**:
   - Nhấp đúp vào file [`run_hive_cli.bat`](run_hive_cli.bat).
   - Hoặc chạy qua dòng lệnh:
     ```powershell
     docker compose exec -it hive-server hive
     ```
     *(Trong giao diện `hive>`, bạn có thể gõ trực tiếp các lệnh như: `USE higgs; SHOW TABLES; SELECT * FROM activity LIMIT 5;`)*


---

## 4. Kiểm tra HDFS trực tiếp trên Web Browser
Trong lúc container đang chạy, bạn có thể mở trình duyệt:
- **HDFS Web UI**: [http://localhost:9870](http://localhost:9870)
- Vào **Utilities** ➔ **Browse the file system** để trực tiếp kiểm tra thư mục `/data/higgs/raw` và `/data/higgs/clean` trên HDFS NameNode.

---

## 5. Dừng hệ thống & Thu hồi RAM
Khi chạy xong, nhấp đúp vào file [`stop.bat`](stop.bat) hoặc gõ:
```powershell
docker compose down
```
Toàn bộ container HDFS sẽ dừng lại và giải phóng 100% bộ nhớ RAM cho hệ điều hành Windows.
