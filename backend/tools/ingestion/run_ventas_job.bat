@echo off
setlocal enabledelayedexpansion

REM PRUEBA MÍNIMA: crear carpeta y escribir un archivo
set "LOG_DIR=C:\Proyectos Justin\Logs\Segmentacion"
mkdir "%LOG_DIR%" 2>nul
echo prueba_ok > "%LOG_DIR%\_prueba_creacion.txt"

REM 1) Ruta al .env (sin hardcodear dentro del Python)
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..\..") do set "PROJECT_ROOT=%%~fI"
set "ENV_FILE=%PROJECT_ROOT%\.env"


REM Rutas relativas al .bat (sin hardcodear el path del proyecto)
set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..\..") do set "PROJECT_ROOT=%%~fI"

set "ENV_FILE=%PROJECT_ROOT%\.env"
set "JOB=%PROJECT_ROOT%\backend\tools\ingestion\ventas_job.py"

REM 2) Cargar variables del .env
for /f "usebackq tokens=1,* delims==" %%A in ("%ENV_FILE%") do (
  set "K=%%A"
  set "V=%%B"
  if not "!K!"=="" (
    if not "!K:~0,1!"=="#" (
      set "!K!=!V!"
    )
  )
)

REM 4) Ejecutar el job
set "PY=C:\Users\auxiliarproyecto\AppData\Local\Python\bin\python.exe"
set "JOB=C:\Proyectos Justin\Codigo Proyectos\ProyectoSegmentacion\backend\tools\ingestion\ventas_job.py"

echo VENTAS_SOURCE_FILE=%VENTAS_SOURCE_FILE%
echo VENTAS_BATCH_SIZE=%VENTAS_BATCH_SIZE%

REM 5) Logs a archivo (uno por ejecución)
set "LOG_DIR=C:\Proyectos Justin\Logs\Segmentacion"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
set "LOG_FILE=%LOG_DIR%\ventas_job_%TS%.log"

echo Log file: %LOG_FILE%

"%PY%" "%JOB%" ^
  --pg-dsn "%POSTGRES_DSN%" ^
  --source-file "%VENTAS_SOURCE_FILE%" ^
  --encoding "%VENTAS_ENCODING%" ^
  --batch-size %VENTAS_BATCH_SIZE% ^
  > "%LOG_FILE%" 2>&1

endlocal
