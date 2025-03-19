#!/bin/bash

# loop number
N=1000
FILE_NAME="loop_test_log.txt"
COMMAND="RUST_BACKTRACE=1 cargo x scheck >> $FILE_NAME 2>&1"

rm -f $FILE_NAME
for ((i=1; i<=N; i++))
do
    echo "Running command for the $i time..."
    echo "Loop $i:" >> $FILE_NAME
    eval $COMMAND
    echo "Command finished for the $i time."
done

echo "All commands have been executed. Check log file for the output."