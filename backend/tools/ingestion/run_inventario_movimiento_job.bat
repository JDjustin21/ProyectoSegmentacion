@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..\..") do set "PROJECT_ROOT=%%~fI"

set "LOG_DIR=%PROJECT_ROOT%\backend\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
set "LOG_FILE=%LOG_DIR%\inventario_movimiento_job_%TS%.log"
set "STATUS_FILE=%LOG_DIR%\_jobs_status.log"

cd /d "%PROJECT_ROOT%"
call "%PROJECT_ROOT%\.venv\Scripts\activate.bat"

set INV_BATCH_SIZE=200
set INV_API_TIMEOUT=120

python -m backend.tools.ingestion.inventario_movimiento_job >> "%LOG_FILE%" 2>&1
set "RC=%ERRORLEVEL%"

if %RC% NEQ 0 (
  echo [%DATE% %TIME%] ERROR inventario_movimiento_job (rc=%RC%) log=%LOG_FILE%>> "%STATUS_FILE%"
  exit /b %RC%
) else (
  echo [%DATE% %TIME%] OK inventario_movimiento_job log=%LOG_FILE%>> "%STATUS_FILE%"
)

endlocal