@echo off
setlocal

REM === 1) Ir a la raíz del proyecto (ajusta esta ruta) ===
cd /d "C:\Proyectos\CodigoProyectos\ProyectoSegmentacion"

REM === 2) Activar venv (ajusta si tu venv tiene otro nombre) ===
call ".venv\Scripts\activate.bat"

REM === 3) Variables de ejecución (puedes ajustar) ===
set INV_BATCH_SIZE=200
set INV_API_TIMEOUT=120

REM === 4) Ejecutar job ===
python -m backend.tools.ingestion.inventario_movimiento_job

REM === 5) Log básico de salida ===
if %ERRORLEVEL% NEQ 0 (
  echo [%DATE% %TIME%] ERROR inventario_movimiento_job >> backend\tools\ingestion\logs_inventario_movimiento_job.txt
  exit /b %ERRORLEVEL%
) else (
  echo [%DATE% %TIME%] OK inventario_movimiento_job >> backend\tools\ingestion\logs_inventario_movimiento_job.txt
)

endlocal