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
set "SCRIPT=%PROJECT_ROOT%\backend\tools\ingestion\tiendas_siesa_job.py"
set "LOG_DIR=%PROJECT_ROOT%\backend\logs\jobs"
set "STATUS_DIR=%PROJECT_ROOT%\backend\logs\status"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%STATUS_DIR%" mkdir "%STATUS_DIR%"

set "LOG_FILE=%LOG_DIR%\tiendas_siesa_job.log"
set "STATUS_FILE=%STATUS_DIR%\tiendas_siesa_job_last_status.txt"
set "LOCK_FILE=%STATUS_DIR%\tiendas_siesa_job.lock"

REM ============================================
REM 3) INICIAR LOG LIMPIO
REM ============================================
> "%LOG_FILE%" echo [%DATE% %TIME%] INICIO LOG tiendas_siesa_job

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
REM 5) IR AL ROOT DEL PROYECTO
REM ============================================
cd /d "%PROJECT_ROOT%"

REM ============================================
REM 6) VARIABLES DEL JOB
REM ============================================
set "SOURCE=\\svr-fs\CREYTEX\GESTION FINANCIERA\BI\4. Tablas Maestras\9. Maestra de Tiendas\MAESTRA_TIENDAS.txt"
set "POSTGRES_DSN=dbname=ProyectoSegmentacion user=postgres password=postgres host=10.10.20.247 port=5432"

REM ============================================
REM 7) VALIDACIONES PREVIAS
REM ============================================
if not exist "%PYTHON_EXE%" (
    echo [%DATE% %TIME%] ERROR: no existe PYTHON_EXE=%PYTHON_EXE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_PYTHON
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 21
)

if not exist "%SCRIPT%" (
    echo [%DATE% %TIME%] ERROR: no existe SCRIPT=%SCRIPT% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_SCRIPT
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 22
)

if not exist "%SOURCE%" (
    echo [%DATE% %TIME%] ERROR: no existe SOURCE=%SOURCE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_NO_SOURCE
    del "%LOCK_FILE%" >nul 2>&1
    exit /b 23
)

REM ============================================
REM 8) EJECUCION DEL JOB
REM ============================================
echo [%DATE% %TIME%] INICIO tiendas_siesa_job.py >> "%LOG_FILE%"
echo [%DATE% %TIME%] SOURCE=%SOURCE% >> "%LOG_FILE%"

"%PYTHON_EXE%" "%SCRIPT%" ^
  --pg-dsn "%POSTGRES_DSN%" ^
  --source-file "%SOURCE%" ^
  --encoding "cp1252" >> "%LOG_FILE%" 2>&1

set "RC=%ERRORLEVEL%"

REM ============================================
REM 9) RESULTADO FINAL
REM ============================================
if %RC% NEQ 0 (
    echo [%DATE% %TIME%] ERROR tiendas_siesa_job rc=%RC% log=%LOG_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo FAILED_%RC%
    del "%LOCK_FILE%" >nul 2>&1
    exit /b %RC%
) else (
    echo [%DATE% %TIME%] OK tiendas_siesa_job log=%LOG_FILE% >> "%LOG_FILE%"
    > "%STATUS_FILE%" echo SUCCESS
)

del "%LOCK_FILE%" >nul 2>&1
endlocal
exit /b 0