#!/bin/bash

set -e

cd ./target/debug

# Setup directories.
mkdir -p integration-test/logs
mkdir -p integration-test/input
mkdir -p integration-test/output

# Launch the master.
echo "Launching Master."
./master --fresh --nodump --port=10008 > integration-test/logs/master.log 2>&1 &
master_pid=$!

local_ip="127.0.0.1"

# Spin up 3 workers.
worker_pids=(0 0 0)
for i in ${!worker_pids[*]}
do
    echo "Launching worker $i"
    ./worker -m "${local_ip}:10008" -i "${local_ip}" --fresh --nodump > integration-test/logs/worker-"${i}".log 2>&1 &
    worker_pids[$i]=$!
done

echo ${worker_pids[*]}

# Create some sample data.
echo "how much wood would a wood chuck chuck if a wood chuck could chuck wood" \
    > integration-test/input/sample.txt

declare -A results
results+=( [how]=1 [much]=1 [wood]=4 [would]=1 [a]=2 [chuck]=4 [could]=1 [if]=1 )

# Sleep to allow the master time to startup.
sleep 1

# Launch a MapReduce using the above data.
./cli -m "${local_ip}:10008" run \
    -b "$PWD"/examples/word-counter \
    -i "$PWD"/integration-test/input \
    -o "$PWD"/integration-test/output

./cli -m "${local_ip}:10008" cluster_status

# Launch the CLI to monitor status.
echo "Launching CLI."
attempt_counter=0
while true
do
    echo "Checking Status using CLI..."
    status=$(./cli -m "${local_ip}:10008" status 2>&1 | grep 'DONE\|FAILED\|IN_PROGRESS\|IN_QUEUE' -o)
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
$(kill -9 ${master_pid} ${worker_pids[*]});

# Verify that the output is correct.
for key in "${!results[@]}"
do
    count=$(grep "[0-9]*" -o integration-test/output/"${key}")
    if [ "${count}" != "${results[${key}]}" ]
    then
        echo "Error. Expected ${key}=${results[${key}]}, but got: $count"
        exit 1
    fi
done

echo "Integration test passed succesfully"

# Cleanup.
rm -rf integration-test
