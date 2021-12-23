@echo off

setlocal

set WORKDIR=%~dp0\..
set ConnectionInfo=%WORKDIR%\long-run\connection-info.properties
set TablesPerThread=3
set TableListFileName=%WORKDIR%\long-run\table-list.txt
set DeleteThreshold=5000
set DeleteOnStart=true
set StartHintValue=1
set RandomString=true
set NumberOfRunsToPause=0
set TimeToPause=1000

pushd %WORKDIR%

@echo on
call %WORKDIR%\run.bat %ConnectionInfo% %TablesPerThread% %TableListFileName% %DeleteThreshold% %DeleteOnStart% %StartHintValue% %RandomString% %NumberOfRunsToPause% %TimeToPause%
@echo off

endlocal
