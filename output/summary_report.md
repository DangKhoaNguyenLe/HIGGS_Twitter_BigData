# BÁO CÁO KẾT QUẢ THỰC NGHIỆM HIGGS TWITTER ANALYSIS

- **Tổng thời gian thực thi**: 505.55 giây (~8.4 phút)
- **Số bản ghi dữ liệu thô (Raw)**: 563,069 dòng
- **Số bản ghi sạch sau tiền xử lý (Clean)**: 563,069 dòng
- **Số dòng rác/lỗi đã loại bỏ**: 0 dòng
- **Tổng số người dùng phân tích (User Features)**: 279,379 users
- **Thuật toán phân cụm**: K-Means (Baseline, K=4)
- **Silhouette Score**: 0.6859
- **Calinski-Harabasz Score**: 9200.50
- **Davies-Bouldin Score**: 1.0353

## 1. Phân bố quy mô các cụm

|   cluster |   user_count |   percentage |
|----------:|-------------:|-------------:|
|         0 |       236779 |        84.75 |
|         1 |          309 |         0.11 |
|         2 |         4694 |         1.68 |
|         3 |        37597 |        13.46 |

## 2. Hồ sơ đặc trưng trung bình theo cụm

|   cluster |   total_interactions |   retweet_count |   mention_count |   reply_count |   unique_targets |   active_days |   active_hours |
|----------:|---------------------:|----------------:|----------------:|--------------:|-----------------:|--------------:|---------------:|
|         0 |                 1.37 |            0.91 |            0.38 |          0.08 |             1.24 |          1    |           1.07 |
|         1 |                65.63 |           29.38 |           30.01 |          6.24 |            29.61 |          4.44 |          11.13 |
|         2 |                14.52 |            8.07 |            5.26 |          1.19 |             9.54 |          2.99 |           5.56 |
|         3 |                 3.99 |            2.44 |            1.28 |          0.27 |             3.19 |          1.94 |           2.47 |

## 3. Danh sách biểu đồ đã xuất bản

- `charts/01_interaction_distribution.png`
- `charts/02_daily_activity.png`
- `charts/03_hourly_activity.png`
- `charts/04_kmeans_elbow_inertia.png`
- `charts/05_kmeans_silhouette.png`
- `charts/06_kmeans_calinski_harabasz.png`
- `charts/07_kmeans_davies_bouldin.png`
- `charts/08_pca_clusters.png`
- `charts/09_cluster_profiles.png`
