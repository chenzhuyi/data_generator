@echo off

rem ===================================================================
rem 1. mvn package
rem 2. mvn dependency:copy-dependencies
rem 3. copy-files.bat
rem 4. ftp to linux and do dos2unix on text files
rem 5. cd long-run && bash long-run.sh
rem ===================================================================

setlocal

set DIST_DIR=FEI

pushd target
if exist %DIST_DIR% (rmdir /s /q %DIST_DIR%)
mkdir %DIST_DIR%
cd %DIST_DIR%
copy ..\..\*.sh .
copy ..\..\*.bat .
copy ..\DataGenerator-1.0-SNAPSHOT.jar .

mkdir dependency
xcopy /s /q ..\dependency dependency

mkdir long-run
xcopy /s /q ..\..\long-run long-run

endlocal
