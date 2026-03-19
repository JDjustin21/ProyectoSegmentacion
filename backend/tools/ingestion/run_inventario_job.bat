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
set "LOG_DIR=%PROJECT_ROOT%\backend\logs\jobs"
set "STATUS_DIR=%PROJECT_ROOT%\backend\logs\status"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%STATUS_DIR%" mkdir "%STATUS_DIR%"

set "LOG_FILE=%LOG_DIR%\inventario_job.log"
set "STATUS_FILE=%STATUS_DIR%\inventario_job_last_status.txt"
set "LOCK_FILE=%STATUS_DIR%\inventario_job.lock"

REM ============================================
REM 3) INICIAR LOG LIMPIO
REM ============================================
> "%LOG_FILE%" echo [%DATE% %TIME%] INICIO LOG inventario_job

REM ============================================
REM 4) EVITAR DOBLE EJECUCION
REM ============================================
if exist "%LOCK_FILE%" (
    echo [%DATE% %TIME%] ERROR: ya existe lock file, posible ejecucion en curso. >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_LOCK
    exit /b 20
)

echo %DATE% %TIME% > "%LOCK_FILE%"

REM ============================================
REM 5) VALIDACIONES PREVIAS
REM ============================================
if not exist "%PYTHON_EXE%" (
    echo [%DATE% %TIME%] ERROR: no existe PYTHON_EXE=%PYTHON_EXE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_PYTHON
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 21
)

if not exist "%PROJECT_ROOT%\backend\tools\ingestion\inventario_job.py" (
    echo [%DATE% %TIME%] ERROR: no existe inventario_job.py >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_SCRIPT
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 22
)

REM ============================================
REM 6) IR AL ROOT DEL PROYECTO
REM ============================================
cd /d "%PROJECT_ROOT%"

REM ============================================
REM 7) VARIABLES DE EJECUCION
REM ============================================
set "INV_BATCH_SIZE=200"
set "INV_API_TIMEOUT=180"

echo [%DATE% %TIME%] INV_BATCH_SIZE=%INV_BATCH_SIZE% >> "%LOG_FILE%"
echo [%DATE% %TIME%] INV_API_TIMEOUT=%INV_API_TIMEOUT% >> "%LOG_FILE%"

REM ============================================
REM 8) EJECUTAR JOB
REM ============================================
echo [%DATE% %TIME%] INICIO python -m backend.tools.ingestion.inventario_job >> "%LOG_FILE%"

"%PYTHON_EXE%" -m backend.tools.ingestion.inventario_job >> "%LOG_FILE%" 2>&1
set "RC=%ERRORLEVEL%"

REM ============================================
REM 9) RESULTADO FINAL
REM ============================================
if %RC% NEQ 0 (
    echo [%DATE% %TIME%] ERROR inventario_job rc=%RC% log=%LOG_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_%RC%
    del "%LOCK_FILE%" >nul 2>&1
    exit /b %RC%
) else (
    echo [%DATE% %TIME%] OK inventario_job log=%LOG_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo SUCCESS
)

del "%LOCK_FILE%" >nul 2>&1
endlocal
exit /b 0