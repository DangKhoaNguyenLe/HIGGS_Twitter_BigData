@echo off
title HIGGS Twitter Big Data Analysis 
echo ======================================================================
echo    HIGGS TWITTER BIG DATA ANALYSIS
echo ======================================================================
echo.

cd /d "%~dp0"

echo [1/3] Dang kiem tra va build Docker image...
docker compose build higgs-analysis
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [LOI] Build Docker image that bai! Kiem tra Docker Desktop da bat chua.
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo [2/3] Dang khoi dong HDFS Cluster va chay main.py dieu phoi toan bo module...
docker compose run --rm higgs-analysis
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [LOI] Qua trinh thuc thi gap loi!
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ======================================================================
echo [3/3] HOAN TAT DONG LOAT TAT CA CAC MODULE THANH CONG!
echo - HDFS Web UI theo doi file    : http://localhost:9870
echo - Toan bo bieu do PNG da luu   : output\charts\
echo - Toan bo file du lieu CSV luu : output\
echo - Bao cao tong hop markdown    : output\summary_report.md
echo ======================================================================
echo.
pause
