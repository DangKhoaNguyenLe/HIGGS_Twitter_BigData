@echo off
title Chay kich ban HiveQL tren Apache Hive
cd /d "%~dp0"
echo ======================================================================
echo    THUC THI KICH BAN HIVEQL (hive_analysis.hql) TREN APACHE HIVE
echo ======================================================================
echo.
echo Dang thuc thi /project/src/hive_analysis.hql tren higgs_hive_server...
docker compose exec hive-server hive -f /project/src/hive_analysis.hql
echo.
echo ======================================================================
echo Hoan tat thuc thi kich ban HiveQL!
echo ======================================================================
pause
