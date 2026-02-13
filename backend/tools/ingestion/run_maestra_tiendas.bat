@echo off
setlocal

REM 1) Python real (no WindowsApps)
set "PYTHON_EXE=C:\Users\auxiliarproyecto\AppData\Local\Python\bin\python.exe"

REM 2) Script del job
set "SCRIPT=C:\Proyectos Justin\Codigo Proyectos\ProyectoSegmentacion\backend\tools\ingestion\maestra_tiendas_job.py"

REM 3) Fuente UNC (no depender de M:)
set "SOURCE=\\svr-fs\CREYTEX\GESTION FINANCIERA\BI\4. Tablas Maestras\9. Maestra de Tiendas\MAESTRA_TIENDAS_POR_LINEA.txt"

REM 4) DSN (igual al de tu .env; aquí queda centralizado para la tarea)
set "POSTGRES_DSN=dbname=Creytex_Segmentacion_V1 user=postgres password=postgres host=localhost port=5432"

REM 5) Carpeta de logs
set "LOGDIR=C:\Creytex\Segmentacion\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

REM 6) Timestamp YYYYMMDD_HHMMSS (formato estable)
set "TS=%DATE:~-4%%DATE:~3,2%%DATE:~0,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%"
set "TS=%TS: =0%"

"%PYTHON_EXE%" "%SCRIPT%" --pg-dsn "%POSTGRES_DSN%" --source-file "%SOURCE%" --encoding "cp1252" >> "%LOGDIR%\maestra_tiendas_%TS%.log" 2>&1

endlocal
