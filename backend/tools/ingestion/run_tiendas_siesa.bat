@echo off
setlocal

set "PYTHON_EXE=C:\Users\auxiliarproyecto\AppData\Local\Python\bin\python.exe"
set "SCRIPT=C:\Proyectos Justin\Codigo Proyectos\ProyectoSegmentacion\backend\tools\ingestion\tiendas_siesa_job.py"

REM Ruta del TXT maestro que trae COD_SIESA (ajusta a la real)
set "SOURCE=\\svr-fs\CREYTEX\GESTION FINANCIERA\BI\4. Tablas Maestras\9. Maestra de Tiendas\MAESTRA_TIENDAS.txt"

set "POSTGRES_DSN=dbname=Creytex_Segmentacion_V1 user=postgres password=postgres host=localhost port=5432"

set "LOGDIR=C:\Creytex\Segmentacion\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

set "TS=%DATE:~-4%%DATE:~3,2%%DATE:~0,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%"
set "TS=%TS: =0%"

"%PYTHON_EXE%" "%SCRIPT%" --pg-dsn "%POSTGRES_DSN%" --source-file "%SOURCE%" --encoding "cp1252" >> "%LOGDIR%\tiendas_siesa_%TS%.log" 2>&1

endlocal
