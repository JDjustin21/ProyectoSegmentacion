@echo off
setlocal

REM === 1) Ir a la raíz del proyecto (ajusta esta ruta) ===
cd /d "C:\Proyectos Justin\Codigo Proyectos\ProyectoSegmentacion"

REM === 2) Activar venv (ajusta si tu venv tiene otro nombre) ===
call ".venv\Scripts\activate.bat"

REM === 3) Variables de ejecución (puedes ajustar) ===
set INV_BATCH_SIZE=200
set INV_API_TIMEOUT=180

REM === 4) Ejecutar job ===
python -m backend.tools.ingestion.inventario_job

REM === 5) Log básico de salida ===
if %ERRORLEVEL% NEQ 0 (
  echo [%DATE% %TIME%] ERROR inventario_job >> backend\tools\ingestion\logs_inventario_job.txt
  exit /b %ERRORLEVEL%
) else (
  echo [%DATE% %TIME%] OK inventario_job >> backend\tools\ingestion\logs_inventario_job.txt
)

if %ERRORLEVEL% NEQ 0 (
   echo [%DATE% %TIME%] ERROR inventario_job >> C:\Proyectos Justin\Codigo Proyectos\ProyectoSegmentacion\backend\tools\ingestion\logs_inventario_job.txt
   exit /b %ERRORLEVEL%
)
endlocal
