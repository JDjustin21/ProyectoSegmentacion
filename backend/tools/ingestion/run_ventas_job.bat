@echo off
setlocal enabledelayedexpansion

REM 1) Resolver PROJECT_ROOT relativo a este .bat (está en backend\tools\ingestion)
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..\..") do set "PROJECT_ROOT=%%~fI"

REM 2) Logs centralizados
set "LOG_DIR=%PROJECT_ROOT%\backend\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
set "LOG_FILE=%LOG_DIR%\ventas_job_%TS%.log"
set "STATUS_FILE=%LOG_DIR%\_jobs_status.log"

REM 3) Cargar .env
set "ENV_FILE=%PROJECT_ROOT%\.env"
for /f "usebackq tokens=1,* delims==" %%A in ("%ENV_FILE%") do (
  set "K=%%A"
  set "V=%%B"
  if not "!K!"=="" (
    if not "!K:~0,1!"=="#" (
      set "!K!=!V!"
    )
  )
)

REM 4) Activar venv y ejecutar job
cd /d "%PROJECT_ROOT%"
call "%PROJECT_ROOT%\.venv\Scripts\activate.bat"

set "JOB=%PROJECT_ROOT%\backend\tools\ingestion\ventas_job.py"

echo VENTAS_SOURCE_FILE=%VENTAS_SOURCE_FILE%>> "%LOG_FILE%"
echo VENTAS_BATCH_SIZE=%VENTAS_BATCH_SIZE%>> "%LOG_FILE%"

python "%JOB%" ^
  --pg-dsn "%POSTGRES_DSN%" ^
  --source-file "%VENTAS_SOURCE_FILE%" ^
  --encoding "%VENTAS_ENCODING%" ^
  --batch-size %VENTAS_BATCH_SIZE% ^
  >> "%LOG_FILE%" 2>&1

set "RC=%ERRORLEVEL%"

if %RC% NEQ 0 (
  echo [%DATE% %TIME%] ERROR ventas_job (rc=%RC%) log=%LOG_FILE%>> "%STATUS_FILE%"
  exit /b %RC%
) else (
  echo [%DATE% %TIME%] OK ventas_job log=%LOG_FILE%>> "%STATUS_FILE%"
)

endlocal