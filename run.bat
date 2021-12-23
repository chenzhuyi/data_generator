@echo off
setlocal

IF EXIST c:\tmp\DataGenerator-shutdown (DEL c:\tmp\DataGenerator-shutdown)

rem -- if run from dev env (v.s. run from files created from copy-files.bat)
IF EXIST target (pushd target)

set CLASSPATH=DataGenerator-1.0-SNAPSHOT.jar;dependency/commons-lang3-3.5.jar;dependency/commons-io-2.5.jar;dependency/commons-dbutils-1.6.jar;dependency/ojdbc6-11.2.0.3.jar;dependency/com.sap.db.ngdbc-1.0.jar

echo Generating sql script ...
REM java -cp DataGenerator-1.0-SNAPSHOT.jar com.sap.hana.dp.datagen.SqlScriptGenerator C:\fei_backup\tmp\Long-Running-SDI\Longrunning_test_source_tables_o2h.txt C:\fei_backup\tmp\Long-Running-SDI\scripts\fei-tables.sql

echo Start data generator ...
REM #Usage: TableDataGenerator <db-connection-info-prop-filename> <table-list-filename> <delete-threshold> <delete-on-start> <start-hint-value>
REM java -cp %CLASSPATH% com.sap.hana.dp.datagen.TableDataGenerator C:\fei_backup\tmp\Long-Running-SDI\connection-info.properties C:\fei_backup\tmp\Long-Running-SDI\Longrunning_test_source_tables_o2h.txt 100 true 0
java -cp %CLASSPATH% com.sap.hana.dp.datagen.TableDataGenerator %*
pause

popd

endlocal
