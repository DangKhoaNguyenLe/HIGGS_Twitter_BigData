@echo off
title Dung Cluster HDFS va Containers
echo ======================================================================
echo    DUNG TOAN BO CLUSTER HDFS VA CONTAINER (GIAI PHONG RAM)
echo ======================================================================
echo.

cd /d "%~dp0"
docker compose down

echo.
echo => Da dung toan bo cluster HDFS va giai phong bo nho RAM thanh cong!
echo.
pause
