#!/bin/bash

set -e

cd ../target/debug

# Setup directories.
mkdir -p integration-test/logs
mkdir -p integration-test/input
mkdir -p integration-test/output

# Launch the master.
echo "Launching Master."
./master --fresh --nodump --port=10008 > integration-test/logs/master.log 2>&1 &
master_pid=$!

# Spin up 3 workers.
worker_pids=(0 0 0)
for i in ${!worker_pids[*]}
do
    echo "Launching worker $i"
    ./worker -m "[::]:10008" > integration-test/logs/worker-"${i}".log 2>&1 &
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
./cli -m "[::]:10008" run \
    -b "$PWD"/examples/word-counter \
    -i "$PWD"/integration-test/input \
    -o "$PWD"/integration-test/output

./cli -m "[::]:10008" cluster_status

# Launch the CLI to monitor status.
echo "Launching CLI."
while true
do
    done_status=$(./cli -m "[::]:10008" status 2>&1 | tee | grep "DONE" -o);
    fail_status=$(./cli -m "[::]:10008" status 2>&1 | tee | grep "FAILED" -o);
    if [ "$done_status" == "DONE" ]; then break; fi
    if [ "$fail_status" == "FAILED" ]; then exit 1; fi
done

# Kill any spawned processes.
kill -9 ${master_pid} ${worker_pids[*]}

# Verify that the output is correct.
for key in "${!results[@]}"
do
    count=$(grep "[0-9]*" -o integration-test/output/"${key}")
    echo "${key}" "${count}"
    if [ "${count}" != "${results[${key}]}" ]
    then
        echo "Error. Expected ${key}=${results[${key}]}, but got: $count"
        exit 1
    fi
done

# Cleanup.
rm -rf integration_test
