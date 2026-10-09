@echo off
title Apache Hive Native CLI
cd /d "%~dp0"
echo ======================================================================
echo    APACHE HIVE NATIVE INTERACTIVE CLI (HIVEQL)
echo ======================================================================
echo.
echo Dang ket noi vao Apache Hive CLI tren container higgs_hive_server...
docker compose exec -it hive-server hive
pause
