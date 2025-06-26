@echo off
REM Disable delayed expansion to allow passwords with ! character
SETLOCAL DISABLEDELAYEDEXPANSION

SET "MYSQL_EXE=C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe"
SET "DB_USER=root"
SET "DB_PASS=1qa@WS3ed!QA2ws#ED"
SET "DB_NAME=accelo_dev"
SET "SQL_DIR=C:\polaris_sync_agent\sql_parts"

echo Importing all .sql files into database [%DB_NAME%]...

FOR %%F IN ("%SQL_DIR%\*.sql") DO (
    echo.
    echo Importing: %%~nxF
    "%MYSQL_EXE%" -u %DB_USER% -p%DB_PASS% %DB_NAME% < "%%F"
)

echo.
echo All SQL files processed.
ENDLOCAL
pause
