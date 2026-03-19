@echo off
setlocal enableextensions enabledelayedexpansion

REM ============================================
REM 1) Resolver PROJECT_ROOT relativo a este .bat
REM ============================================
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..\..") do set "PROJECT_ROOT=%%~fI"

REM ============================================
REM 2) Rutas base
REM ============================================
set "PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "JOB=%PROJECT_ROOT%\backend\tools\ingestion\ventas_job.py"
set "ENV_FILE=%PROJECT_ROOT%\.env"

set "LOG_DIR=%PROJECT_ROOT%\backend\logs\jobs"
set "STATUS_DIR=%PROJECT_ROOT%\backend\logs\status"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%STATUS_DIR%" mkdir "%STATUS_DIR%"

set "LOG_FILE=%LOG_DIR%\ventas_job.log"
set "STATUS_FILE=%STATUS_DIR%\ventas_job_last_status.txt"
set "LOCK_FILE=%STATUS_DIR%\ventas_job.lock"

REM ============================================
REM 3) INICIAR LOG LIMPIO
REM ============================================
> "%LOG_FILE%" echo [%DATE% %TIME%] INICIO LOG ventas_job

REM ============================================
REM 4) Lock para evitar doble corrida
REM ============================================
if exist "%LOCK_FILE%" (
    echo [%DATE% %TIME%] ERROR: ya existe lock file, posible ejecucion en curso. >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_LOCK
    exit /b 20
)

echo %DATE% %TIME% > "%LOCK_FILE%"

REM ============================================
REM 5) Validaciones base
REM ============================================
if not exist "%PYTHON_EXE%" (
    echo [%DATE% %TIME%] ERROR: no existe PYTHON_EXE=%PYTHON_EXE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_PYTHON
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 21
)

if not exist "%JOB%" (
    echo [%DATE% %TIME%] ERROR: no existe JOB=%JOB% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_SCRIPT
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 22
)

if not exist "%ENV_FILE%" (
    echo [%DATE% %TIME%] ERROR: no existe ENV_FILE=%ENV_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_ENV
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 23
)

REM ============================================
REM 6) Cargar variables desde .env
REM ============================================
for /f "usebackq tokens=1,* delims==" %%A in ("%ENV_FILE%") do (
    set "K=%%A"
    set "V=%%B"
    if not "!K!"=="" (
        if not "!K:~0,1!"=="#" (
            set "!K!=!V!"
        )
    )
)

REM ============================================
REM 7) Validar variables necesarias
REM ============================================
if "%POSTGRES_DSN%"=="" (
    echo [%DATE% %TIME%] ERROR: POSTGRES_DSN vacio en .env >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_DSN
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 24
)

if "%VENTAS_SOURCE_FILE%"=="" (
    echo [%DATE% %TIME%] ERROR: VENTAS_SOURCE_FILE vacio en .env >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_SOURCE
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 25
)

if "%VENTAS_ENCODING%"=="" set "VENTAS_ENCODING=cp1252"
if "%VENTAS_BATCH_SIZE%"=="" set "VENTAS_BATCH_SIZE=5000"

REM ============================================
REM 8) Validar archivo fuente
REM ============================================
if not exist "%VENTAS_SOURCE_FILE%" (
    echo [%DATE% %TIME%] ERROR: no existe VENTAS_SOURCE_FILE=%VENTAS_SOURCE_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_SOURCE_NOT_FOUND
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 26
)

REM ============================================
REM 9) Ejecutar job
REM ============================================
cd /d "%PROJECT_ROOT%"

echo [%DATE% %TIME%] INICIO ventas_job.py >> "%LOG_FILE%"
echo POSTGRES_DSN=%POSTGRES_DSN% >> "%LOG_FILE%"
echo VENTAS_SOURCE_FILE=%VENTAS_SOURCE_FILE% >> "%LOG_FILE%"
echo VENTAS_ENCODING=%VENTAS_ENCODING% >> "%LOG_FILE%"
echo VENTAS_BATCH_SIZE=%VENTAS_BATCH_SIZE% >> "%LOG_FILE%"

"%PYTHON_EXE%" "%JOB%" ^
  --pg-dsn "%POSTGRES_DSN%" ^
  --source-file "%VENTAS_SOURCE_FILE%" ^
  --encoding "%VENTAS_ENCODING%" ^
  --batch-size %VENTAS_BATCH_SIZE% ^
  >> "%LOG_FILE%" 2>&1

set "RC=%ERRORLEVEL%"

REM ============================================
REM 10) Resultado final
REM ============================================
if %RC% NEQ 0 (
    echo [%DATE% %TIME%] ERROR ventas_job rc=%RC% log=%LOG_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_%RC%
    del "%LOCK_FILE%" >nul 2>&1
    exit /b %RC%
) else (
    echo [%DATE% %TIME%] OK ventas_job log=%LOG_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo SUCCESS
)

del "%LOCK_FILE%" >nul 2>&1
endlocal
exit /b 0