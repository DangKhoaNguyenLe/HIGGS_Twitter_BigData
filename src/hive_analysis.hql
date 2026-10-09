
-- [1] Khởi tạo Database và Bảng Hive trỏ vào thư mục Parquet trên HDFS
CREATE DATABASE IF NOT EXISTS higgs COMMENT 'Co so du lieu phan tich HIGGS Twitter';
USE higgs;

DROP TABLE IF EXISTS activity;

CREATE EXTERNAL TABLE IF NOT EXISTS activity (
    user_a BIGINT,
    user_b BIGINT,
    ts BIGINT,
    `type` STRING,
    type_name STRING,
    `datetime` STRING,
    `hour` INT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/data/higgs/clean';

-- Đồng bộ siêu dữ liệu phân vùng partition
MSCK REPAIR TABLE activity;

-- Xem trước 5 dòng dữ liệu
SELECT * FROM activity LIMIT 5;

-- Đếm tổng số dòng
SELECT COUNT(*) AS total_records FROM activity;

-- [2] Thống kê tổng quan người dùng và lượng tương tác
SELECT 
    COUNT(*)               AS tong_tuong_tac,
    COUNT(DISTINCT user_a) AS so_user_thuc_hien,
    COUNT(DISTINCT user_b) AS so_user_duoc_tuong_tac
FROM activity;

-- [3] Tỷ lệ phân bố các loại tương tác (Window OVER)
SELECT 
    type_name,
    so_luong,
    ROUND(100.0 * so_luong / SUM(so_luong) OVER (), 2) AS ty_le_phan_tram
FROM (
    SELECT type_name, COUNT(*) AS so_luong
    FROM activity
    GROUP BY type_name
) sub
ORDER BY so_luong DESC;

-- [4] Thống kê tương tác theo từng ngày
SELECT 
    dt,
    SUM(CASE WHEN `type` = 'RT' THEN 1 ELSE 0 END) AS retweet,
    SUM(CASE WHEN `type` = 'MT' THEN 1 ELSE 0 END) AS mention,
    SUM(CASE WHEN `type` = 'RE' THEN 1 ELSE 0 END) AS reply,
    COUNT(*)                                       AS tong
FROM activity
GROUP BY dt
ORDER BY dt;

-- [5] Thống kê tương tác theo từng khung giờ trong ngày (0h - 23h)
SELECT 
    `hour`,
    SUM(CASE WHEN `type` = 'RT' THEN 1 ELSE 0 END) AS retweet,
    SUM(CASE WHEN `type` = 'MT' THEN 1 ELSE 0 END) AS mention,
    SUM(CASE WHEN `type` = 'RE' THEN 1 ELSE 0 END) AS reply,
    COUNT(*)                                       AS tong
FROM activity
GROUP BY `hour`
ORDER BY `hour`;

-- [6] Top 5 user được Retweet nhiều nhất (Lan truyền thông tin)
SELECT user_b AS user_id, COUNT(*) AS so_lan_duoc_retweet
FROM activity 
WHERE `type` = 'RT'
GROUP BY user_b 
ORDER BY so_lan_duoc_retweet DESC 
LIMIT 5;

-- [7] Top 5 user hoạt động tích cực nhất (Gửi tương tác nhiều nhất)
SELECT user_a AS user_id, COUNT(*) AS tong_hanh_dong
FROM activity 
GROUP BY user_a 
ORDER BY tong_hanh_dong DESC 
LIMIT 5;

-- [8] Xây dựng bảng đặc trưng người dùng 'user_features' cho Machine Learning (K-Means)
DROP TABLE IF EXISTS user_features;

CREATE TABLE user_features
STORED AS PARQUET
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
GROUP BY user_a;

-- [9] Phân tích tốc độ tăng trưởng tương tác theo ngày (Window function LAG)
SELECT 
    dt, 
    tong, 
    tong_ngay_truoc,
    tong - tong_ngay_truoc AS chenh_lech,
    ROUND(100.0 * (tong - tong_ngay_truoc) / tong_ngay_truoc, 2) AS tang_truong_pct
FROM (
    SELECT 
        dt, 
        tong, 
        LAG(tong) OVER (ORDER BY dt) AS tong_ngay_truoc
    FROM (
        SELECT dt, COUNT(*) AS tong 
        FROM activity 
        GROUP BY dt
    ) d
) x 
ORDER BY dt;

-- [10] So sánh 3 giai đoạn: Trước, Trong và Sau sự kiện công bố hạt Higgs (04/07/2012)
SELECT 
    period,
    COUNT(*) AS tong_tuong_tac,
    COUNT(DISTINCT dt) AS so_ngay,
    ROUND(COUNT(*) / COUNT(DISTINCT dt), 0) AS trung_binh_ngay
FROM (
    SELECT 
        dt,
        CASE 
            WHEN dt <  '2012-07-04' THEN '1_Truoc_su_kien'
            WHEN dt =  '2012-07-04' THEN '2_Ngay_cong_bo_04_07'
            ELSE '3_Sau_su_kien' 
        END AS period
    FROM activity
) p 
GROUP BY period 
ORDER BY period;
