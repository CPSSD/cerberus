#!/bin/bash

set -e

cd ./target/debug

# Setup directories.
mkdir -p state-integration-test/logs
mkdir -p state-integration-test/input
mkdir -p state-integration-test/output
mkdir -p state-integration-test/state

# Launch the master.
echo "Launching Master."
./master --fresh --state-location="$PWD"/state-integration-test/state --port=10009 > state-integration-test/logs/master.log 2>&1 &
master_pid=$!

local_ip="127.0.0.1"

# Create some sample data.
echo "how much wood would a wood chuck chuck if a wood chuck could chuck wood" \
    > state-integration-test/input/sample.txt

# SLeep to allow the master time to start up.
sleep 2

declare -A results
results+=( [how]=1 [much]=1 [wood]=4 [would]=1 [a]=2 [chuck]=4 [could]=1 [if]=1 )

# Launch a MapReduce using the above data.
./cli -m "${local_ip}:10009" run \
    -b "$PWD"/examples/word-counter \
    -i "$PWD"/state-integration-test/input \
    -o "$PWD"/state-integration-test/output

./cli -m "${local_ip}:10009" cluster_status

# Sleep to allow the master time to save the state.
sleep 10

# Kill the master.
$(kill -9 ${master_pid});

sleep 1

echo "Relaunching Master."
./master --state-location="$PWD"/state-integration-test/state --port=10009 > state-integration-test/logs/master2.log 2>&1 &
master_pid=$!

sleep 1

# Launch a worker.
echo "Launching worker"
./worker -m "${local_ip}:10009" -i "${local_ip}" --fresh --nodump > state-integration-test/logs/worker.log 2>&1 &
worker_pid=$!

# Launch the CLI to monitor status.
echo "Launching CLI."
attempt_counter=0
while true
do
    echo "Checking Status using CLI..."
    status=$(./cli -m "${local_ip}:10009" status 2>&1 | grep 'DONE\|FAILED\|IN_PROGRESS\|IN_QUEUE' -o)
    echo "    - MapReduce Status: " $status
    if [ "$status" == "DONE" ] || [ "$status" == "FAILED" ]; then break; fi

    if [ $attempt_counter -gt 15 ]; then
        echo "Error: Unable to reach an expected status after 15 iterations"
        break;
    fi
    sleep 1
    attempt_counter+=1
done

# Kill any spawned processes.
$(kill -9 ${master_pid} ${worker_pid});

# Verify that the output is correct.
for key in "${!results[@]}"
do
    count=$(grep "[0-9]*" -o state-integration-test/output/"${key}")
    if [ "${count}" != "${results[${key}]}" ]
    then
        echo "Error. Expected ${key}=${results[${key}]}, but got: $count"
        exit 1
    fi
done

echo "Integration test passed succesfully"

# Cleanup.
rm -rf state-integration-test
