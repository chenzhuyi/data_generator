#!/bin/bash

if [ -f "/tmp/DataGenerator-shutdown" ]; then
    rm "/tmp/DataGenerator-shutdown"
fi

#export JAVA_HOME="`pwd`/../download/jdk1.8.0_121"
cd "target"
CLASSPATH=.:DataGenerator-1.0-SNAPSHOT.jar:alternateLocation/commons-lang3-3.5.jar:alternateLocation/commons-io-2.5.jar:alternateLocation/commons-dbutils-1.6.jar:alternateLocation/ojdbc6-11.2.0.3.jar:alternateLocation/com.sap.db.jdbc-1.0.jar

echo "Generating sql script ..."
#java -cp DataGenerator-1.0-SNAPSHOT.jar com.sap.hana.dp.datagen.SqlScriptGenerator Longrunning_test_source_tables_o2h.txt fei-tables.sql

echo Start data generator ...
#Usage: TableDataGenerator <db-connection-info-prop-filename> <table-list-filename> <delete-threshold> <delete-on-start> <start-hint-value>
#"$JAVA_HOME/bin/java" -cp "$CLASSPATH" com.sap.hana.dp.datagen.TableDataGenerator connection-info.properties Longrunning_test_source_tables_o2h.txt 100000 true 1

"$JAVA_HOME/bin/java" -cp "$CLASSPATH" com.sap.hana.dp.datagen.ParallelTableDataGenerator $@
