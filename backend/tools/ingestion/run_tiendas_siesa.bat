@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..\..") do set "PROJECT_ROOT=%%~fI"

set "LOG_DIR=%PROJECT_ROOT%\backend\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
set "LOG_FILE=%LOG_DIR%\tiendas_siesa_%TS%.log"
set "STATUS_FILE=%LOG_DIR%\_jobs_status.log"

cd /d "%PROJECT_ROOT%"
call "%PROJECT_ROOT%\.venv\Scripts\activate.bat"

set "SCRIPT=%PROJECT_ROOT%\backend\tools\ingestion\tiendas_siesa_job.py"
set "SOURCE=\\svr-fs\CREYTEX\GESTION FINANCIERA\BI\4. Tablas Maestras\9. Maestra de Tiendas\MAESTRA_TIENDAS.txt"
set "POSTGRES_DSN=dbname=Creytex_Segmentacion_V1 user=postgres password=postgres host=localhost port=5432"

python "%SCRIPT%" --pg-dsn "%POSTGRES_DSN%" --source-file "%SOURCE%" --encoding "cp1252" >> "%LOG_FILE%" 2>&1
set "RC=%ERRORLEVEL%"

if %RC% NEQ 0 (
  echo [%DATE% %TIME%] ERROR tiendas_siesa_job (rc=%RC%) log=%LOG_FILE%>> "%STATUS_FILE%"
  exit /b %RC%
) else (
  echo [%DATE% %TIME%] OK tiendas_siesa_job log=%LOG_FILE%>> "%STATUS_FILE%"
)

endlocal