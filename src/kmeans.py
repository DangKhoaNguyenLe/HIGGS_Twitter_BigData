# -*- coding: utf-8 -*-
"""
Bước 5: Phân cụm người dùng bằng K-Means (Machine Learning)
Đọc dữ liệu duy nhất từ bảng Hive: user_features.csv
Tối ưu hóa bộ nhớ cho máy RAM 8GB.
"""

import sys
import time
from pathlib import Path
import numpy as np
import pandas as pd

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
from sklearn.decomposition import PCA

try:
    import cuml
    from cuml.cluster import KMeans as cuKMeans
    HAS_CUML = True
    print("=> [K-MEANS] Phát hiện GPU cuML: Sử dụng GPU K-Means")
except ImportError:
    from sklearn.cluster import KMeans as cuKMeans
    HAS_CUML = False
    print("=> [K-MEANS] Môi trường CPU Docker: Tự động dùng Scikit-Learn K-Means (Tối ưu RAM)")

sys.path.append(str(Path(__file__).resolve().parent))
from config import (
    USER_FEATURES_CSV, USER_CLUSTERS_CSV, KMEANS_METRICS_CSV,
    CLUSTER_PROFILE_CSV, CHARTS_DIR, print_banner
)

FEATURE_COLS = [
    "total_interactions", "retweet_count", "mention_count", "reply_count",
    "unique_targets", "active_days", "active_hours"
]

def make_stratified_metric_indices(labels, sample_size=20000, random_state=42):
    labels = np.asarray(labels).reshape(-1)
    n = len(labels)
    sample_size = min(int(sample_size), n)
    unique_labels = np.unique(labels)
    rng = np.random.RandomState(random_state)
    selected = []
    for label in unique_labels:
        cluster_idx = np.flatnonzero(labels == label)
        selected.append(rng.choice(cluster_idx, size=1, replace=False))
    selected = np.concatenate(selected)
    remaining = sample_size - len(selected)
    if remaining > 0:
        mask = np.ones(n, dtype=bool)
        mask[selected] = False
        pool = np.flatnonzero(mask)
        extra = rng.choice(pool, size=remaining, replace=False)
        selected = np.concatenate([selected, extra])
    rng.shuffle(selected)
    return selected

def run_kmeans(csv_path=None, best_k=4, use_log1p_final=False):
    """Huấn luyện và đánh giá K-Means trên tập đặc trưng người dùng."""
    print_banner("PHÂN CỤM NGƯỜI DÙNG BẰNG K-MEANS (CPU/GPU ADAPTIVE)")

    input_file = Path(csv_path) if csv_path else USER_FEATURES_CSV
    if not input_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file đặc trưng: {input_file}")

    print(f"-> Nguồn dữ liệu đặc trưng: {input_file.name}")
    user_features = pd.read_csv(input_file)
    for col in FEATURE_COLS:
        user_features[col] = pd.to_numeric(user_features[col], errors="coerce")
    user_features = user_features.dropna(subset=FEATURE_COLS).copy()
    print(f"-> Tổng số người dùng hợp lệ: {len(user_features):,}")

    X_features = user_features[FEATURE_COLS].to_numpy(dtype=np.float32)

    # 1. Baseline
    scaler_baseline = StandardScaler()
    X_baseline = scaler_baseline.fit_transform(X_features).astype(np.float32)

    # 2. Log1p
    X_log1p_features = np.log1p(X_features)
    scaler_log1p = StandardScaler()
    X_log1p = scaler_log1p.fit_transform(X_log1p_features).astype(np.float32)

    K_RANGE = range(2, 9)
    RANDOM_STATE = 42
    METRIC_SAMPLE_SIZE = 20_000

    def evaluate_version(X, version_name):
        results = []
        for k in K_RANGE:
            t0 = time.time()
            if HAS_CUML:
                model = cuKMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10, max_iter=300)
                labels_raw = model.fit_predict(X)
                labels = np.asarray(labels_raw.get() if hasattr(labels_raw, "get") else labels_raw).reshape(-1)
                inertia = float(model.inertia_)
            else:
                model = cuKMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10, max_iter=300)
                labels = model.fit_predict(X)
                inertia = float(model.inertia_)

            metric_idx = make_stratified_metric_indices(labels, METRIC_SAMPLE_SIZE, random_state=RANDOM_STATE + k)
            X_sample = X[metric_idx]
            labels_sample = labels[metric_idx]

            sil = silhouette_score(X_sample, labels_sample)
            ch = calinski_harabasz_score(X_sample, labels_sample)
            db = davies_bouldin_score(X_sample, labels_sample)
            elapsed = time.time() - t0

            results.append({
                "Version": version_name,
                "K": k,
                "Inertia": inertia,
                "Silhouette": sil,
                "Calinski_Harabasz": ch,
                "Davies_Bouldin": db,
                "Time_sec": elapsed
            })
            print(f"[{version_name:>8}] K={k} | Sil={sil:.4f} | CH={ch:8.1f} | DB={db:.4f} | Time={elapsed:.2f}s")
        return pd.DataFrame(results)

    print("\n--- 1. Đánh giá mô hình BASELINE ---")
    res_base = evaluate_version(X_baseline, "Baseline")

    print("\n--- 2. Đánh giá mô hình LOG1P ---")
    res_log1p = evaluate_version(X_log1p, "Log1p")

    results_df = pd.concat([res_base, res_log1p], ignore_index=True)
    results_df.to_csv(KMEANS_METRICS_CSV, index=False)
    print(f"=> Đã lưu bảng so sánh metrics: {KMEANS_METRICS_CSV.name}")

    metrics_plots = [
        ("Inertia", "Elbow / Inertia", "04_kmeans_elbow_inertia.png"),
        ("Silhouette", f"Silhouette Score (sample={METRIC_SAMPLE_SIZE:,})", "05_kmeans_silhouette.png"),
        ("Calinski_Harabasz", f"Calinski-Harabasz Score (sample={METRIC_SAMPLE_SIZE:,})", "06_kmeans_calinski_harabasz.png"),
        ("Davies_Bouldin", f"Davies-Bouldin Score (sample={METRIC_SAMPLE_SIZE:,})", "07_kmeans_davies_bouldin.png"),
    ]

    for metric_col, title_text, file_name in metrics_plots:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        for ver, df_sub in results_df.groupby("Version"):
            ax.plot(df_sub["K"], df_sub[metric_col], marker="o", linewidth=2, label=ver)
        ax.set_title(title_text, fontweight='bold')
        ax.set_xlabel("Số cụm K")
        ax.set_ylabel(metric_col)
        ax.set_xticks(list(K_RANGE))
        ax.legend()
        ax.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / file_name, dpi=300)
        plt.close()
        print(f"=> Đã lưu biểu đồ: {file_name}")

    X_final = X_log1p if use_log1p_final else X_baseline
    ver_final = "Log1p" if use_log1p_final else "Baseline"

    print(f"\n--- 3. Huấn luyện mô hình K-Means tối ưu ({ver_final}, K={best_k}) ---")
    final_model = cuKMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=10, max_iter=300)
    final_labels = final_model.fit_predict(X_final)
    if hasattr(final_labels, "get"):
        final_labels = final_labels.get()
    final_labels = np.asarray(final_labels).reshape(-1)

    user_features["cluster"] = final_labels

    metric_idx_final = make_stratified_metric_indices(final_labels, METRIC_SAMPLE_SIZE, random_state=RANDOM_STATE)
    fin_sil = silhouette_score(X_final[metric_idx_final], final_labels[metric_idx_final])
    fin_ch = calinski_harabasz_score(X_final[metric_idx_final], final_labels[metric_idx_final])
    fin_db = davies_bouldin_score(X_final[metric_idx_final], final_labels[metric_idx_final])

    print(f"Chỉ số mô hình cuối (K={best_k}):")
    print(f"  - Silhouette        : {fin_sil:.4f}")
    print(f"  - Calinski-Harabasz : {fin_ch:.2f}")
    print(f"  - Davies-Bouldin    : {fin_db:.4f}")

    pca = PCA(n_components=2, random_state=RANDOM_STATE)
    X_pca = pca.fit_transform(X_final[metric_idx_final])
    labels_pca = final_labels[metric_idx_final]

    plt.figure(figsize=(9, 6))
    scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=labels_pca, cmap='viridis', s=8, alpha=0.5)
    plt.title(f"Phân cụm người dùng HIGGS qua PCA 2D ({ver_final}, K={best_k})", fontsize=13, fontweight='bold')
    plt.xlabel(f"PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)")
    plt.ylabel(f"PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)")
    plt.colorbar(scatter, label="Cluster")
    plt.tight_layout()
    chart8_path = CHARTS_DIR / "08_pca_clusters.png"
    plt.savefig(chart8_path, dpi=300)
    plt.close()
    print(f"=> Đã lưu biểu đồ: {chart8_path.name}")

    cluster_profile = user_features.groupby("cluster")[FEATURE_COLS].mean().round(2)
    cluster_profile.to_csv(CLUSTER_PROFILE_CSV)
    print(f"=> Đã lưu hồ sơ đặc trưng theo cụm: {CLUSTER_PROFILE_CSV.name}")

    cluster_profile.T.plot(kind="bar", figsize=(12, 6))
    plt.title("Giá trị trung bình các đặc trưng theo từng cụm người dùng", fontsize=13, fontweight='bold')
    plt.xlabel("Đặc trưng hành vi")
    plt.ylabel("Giá trị trung bình")
    plt.xticks(rotation=45)
    plt.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    chart9_path = CHARTS_DIR / "09_cluster_profiles.png"
    plt.savefig(chart9_path, dpi=300)
    plt.close()
    print(f"=> Đã lưu biểu đồ: {chart9_path.name}")

    cluster_size = user_features["cluster"].value_counts().sort_index().rename("user_count").to_frame()
    cluster_size["percentage"] = (cluster_size["user_count"] / len(user_features) * 100).round(2)
    print("\nThống kê quy mô các cụm người dùng:")
    print(cluster_size)

    user_features.to_csv(USER_CLUSTERS_CSV, index=False)
    print(f"=> Đã lưu kết quả phân cụm toàn bộ người dùng: {USER_CLUSTERS_CSV.name}")

    return {
        "best_k": best_k,
        "version": ver_final,
        "silhouette": fin_sil,
        "calinski_harabasz": fin_ch,
        "davies_bouldin": fin_db,
        "cluster_size": cluster_size,
        "cluster_profile": cluster_profile
    }

if __name__ == "__main__":
    run_kmeans()
