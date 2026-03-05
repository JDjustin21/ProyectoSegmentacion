@echo off
setlocal enabledelayedexpansion

REM 1) Resolver PROJECT_ROOT relativo a este .bat
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..\..") do set "PROJECT_ROOT=%%~fI"

REM 2) Logs centralizados
set "LOG_DIR=%PROJECT_ROOT%\backend\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
set "LOG_FILE=%LOG_DIR%\inventario_job_%TS%.log"
set "STATUS_FILE=%LOG_DIR%\_jobs_status.log"

REM 3) Ir a root y activar venv (sin hardcodear ruta completa)
cd /d "%PROJECT_ROOT%"
call "%PROJECT_ROOT%\.venv\Scripts\activate.bat"

REM 4) Variables de ejecución
set INV_BATCH_SIZE=200
set INV_API_TIMEOUT=180

REM 5) Ejecutar job (capturar stdout+stderr)
python -m backend.tools.ingestion.inventario_job >> "%LOG_FILE%" 2>&1
set "RC=%ERRORLEVEL%"

REM 6) Estado resumido (centralizado)
if %RC% NEQ 0 (
  echo [%DATE% %TIME%] ERROR inventario_job (rc=%RC%) log=%LOG_FILE%>> "%STATUS_FILE%"
  exit /b %RC%
) else (
  echo [%DATE% %TIME%] OK inventario_job log=%LOG_FILE%>> "%STATUS_FILE%"
)

endlocal