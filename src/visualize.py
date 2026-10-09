# -*- coding: utf-8 -*-
"""
Bước 4: Trực quan hóa kết quả phân tích mạng xã hội
Mục [3.7]: Biểu đồ tròn tương tác, biểu đồ xu hướng theo ngày và phân bố 24 khung giờ
"""

import sys
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append(str(Path(__file__).resolve().parent))
from config import CHARTS_DIR, OUTPUT_DIR, print_banner

def run_visualize(pdf_type=None, pdf_daily=None, pdf_hourly=None):
    """Vẽ và lưu biểu đồ phân tích thống kê."""
    print_banner("TRỰC QUAN HÓA KẾT QUẢ PHÂN TÍCH (CHARTS)")
    sns.set_theme(style='whitegrid')

    if pdf_type is None and (OUTPUT_DIR / "stats_type.csv").exists():
        pdf_type = pd.read_csv(OUTPUT_DIR / "stats_type.csv")
    if pdf_daily is None and (OUTPUT_DIR / "stats_daily.csv").exists():
        pdf_daily = pd.read_csv(OUTPUT_DIR / "stats_daily.csv")
    if pdf_hourly is None and (OUTPUT_DIR / "stats_hourly.csv").exists():
        pdf_hourly = pd.read_csv(OUTPUT_DIR / "stats_hourly.csv")

    charts_created = []

    # Chart 1: Biểu đồ tròn tỷ lệ tương tác
    if pdf_type is not None:
        plt.figure(figsize=(7, 7))
        plt.pie(pdf_type['so_luong'], labels=pdf_type['type_name'], autopct='%1.1f%%',
                startangle=140, colors=['#4285F4', '#34A853', '#FBBC05'])
        plt.title('Tỷ lệ phân bố các loại tương tác Twitter (HIGGS Dataset)', fontsize=14, fontweight='bold')
        plt.tight_layout()
        c1 = CHARTS_DIR / "01_interaction_distribution.png"
        plt.savefig(c1, dpi=300)
        plt.close()
        charts_created.append(c1.name)
        print(f"=> Đã lưu biểu đồ: {c1.name}")

    # Chart 2: Diễn biến theo ngày
    if pdf_daily is not None:
        plt.figure(figsize=(10, 5))
        plt.plot(pdf_daily['dt'].astype(str), pdf_daily['tong'], marker='o', color='#EA4335', linewidth=2.5, label='Tổng tương tác')
        if '2012-07-04' in pdf_daily['dt'].values:
            idx = list(pdf_daily['dt'].values).index('2012-07-04')
            plt.axvline(x=idx, color='blue', linestyle='--', linewidth=2, label='Ngày công bố hạt Higgs (04/07)')
        plt.xticks(rotation=45)
        plt.title('Diễn biến tương tác Twitter theo ngày', fontsize=14, fontweight='bold')
        plt.xlabel('Ngày')
        plt.ylabel('Số lượng tương tác')
        plt.legend()
        plt.tight_layout()
        c2 = CHARTS_DIR / "02_daily_activity.png"
        plt.savefig(c2, dpi=300)
        plt.close()
        charts_created.append(c2.name)
        print(f"=> Đã lưu biểu đồ: {c2.name}")

    # Chart 3: Phân bố theo giờ trong ngày
    if pdf_hourly is not None:
        plt.figure(figsize=(10, 5))
        plt.bar(pdf_hourly['hour'], pdf_hourly['tong'], color='#4285F4', alpha=0.85)
        plt.title('Phân bố tương tác theo các khung giờ trong ngày (UTC)', fontsize=14, fontweight='bold')
        plt.xlabel('Giờ (0 - 23h UTC)')
        plt.ylabel('Số lượng tương tác')
        plt.xticks(range(0, 24))
        plt.tight_layout()
        c3 = CHARTS_DIR / "03_hourly_activity.png"
        plt.savefig(c3, dpi=300)
        plt.close()
        charts_created.append(c3.name)
        print(f"=> Đã lưu biểu đồ: {c3.name}")

    return charts_created

if __name__ == "__main__":
    run_visualize()
