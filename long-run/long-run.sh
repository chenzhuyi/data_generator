#!/bin/bash

WORKDIR="/Users/i327631/Documents/DataGenerator_source"
ParallelNumPerThread=1
ConnectionInfo="$WORKDIR/long-run/connection-info.properties"
TableListFileName="$WORKDIR/long-run/table-list.txt"
DeleteThreshold=0
DeleteOnStart=false
MaxInsertCountPerThread=6000
MaxNumberRowsToGenPerRun=1000
MaxUpdateCountPerThread=0
EnableLog=true
SpecifyValues="$WORKDIR/long-run/specify-values.txt"

cd "$WORKDIR"
bash "run.sh" $ParallelNumPerThread "$ConnectionInfo" "$TableListFileName" $DeleteThreshold \
$DeleteOnStart $MaxInsertCountPerThread $MaxNumberRowsToGenPerRun \
$MaxUpdateCountPerThread $EnableLog "$SpecifyValues"\
