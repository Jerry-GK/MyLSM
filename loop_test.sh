#!/bin/bash

# loop number
N=100

for ((i=1; i<=N; i++))
do
    echo "Running command for the $i time..."
    echo "Loop $i:" >> log_loop.txt
    RUST_BACKTRACE=1 cargo test week2_day4 -- --nocapture >> log_loop.txt 2>&1
    echo "Command finished for the $i time."
done

echo "All commands have been executed. Check log_loop.txt for the output."