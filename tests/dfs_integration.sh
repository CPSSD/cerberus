#!/bin/bash

set -e

cd ./target/debug

# Setup directories.
mkdir -p dfs-integration-test/logs
mkdir -p dfs-integration-test/input
mkdir -p dfs-integration-test/output
mkdir -p dfs-integration-test/master-state
mkdir -p dfs-integration-test/worker1-state
mkdir -p dfs-integration-test/worker2-state
mkdir -p dfs-integration-test/worker3-state

# Launch the master.
echo "Launching Master."
./master --dfs --fresh --state-location="$PWD"/dfs-integration-test/master-state \
    --storage-location="$PWD"/dfs-integration-test/master-state \
    --port=10011 > dfs-integration-test/logs/master.log 2>&1 &
master_pid=$!

local_ip="127.0.0.1"

# Spin up 3 workers.
worker_pids=(0 0 0)
for i in ${!worker_pids[*]}
do
    echo "Launching worker $i"
    ./worker -m "${local_ip}:10011" -i "${local_ip}" --dfs --fresh \
        --storage-location="$PWD"/dfs-integration-test/worker"${i}"-state \
        --state-location="$PWD"/dfs-integration-test/worker"${i}"-state > dfs-integration-test/logs/worker-"${i}".log 2>&1 &
    worker_pids[$i]=$!
done

echo ${worker_pids[*]}

# Create some sample data.
echo "how much wood would a wood chuck chuck if a wood chuck could chuck wood" \
    > dfs-integration-test/input/sample.txt

declare -A results
results+=( [how]=1 [much]=1 [wood]=4 [would]=1 [a]=2 [chuck]=4 [could]=1 [if]=1 )

# SLeep to allow the master time to start up.
sleep 2

# Upload binary to the cluster.
./cli -m "${local_ip}:10011" upload \
    -l "$PWD"/examples/word-counter \
    -r /binary/

# Upload input to the cluster.
./cli -m "${local_ip}:10011" upload \
    -l "$PWD"/dfs-integration-test/input \
    -r /dfs/input/

./cli -m "${local_ip}:10011" cluster_status

# Sleep to allow the master and workers time to save the state.
sleep 10

# Kill the master and workers.
$(kill -9 ${master_pid} ${worker_pids[*]});

sleep 1

echo "Relaunching Master."
./master --dfs --state-location="$PWD"/dfs-integration-test/master-state \
    --storage-location="$PWD"/dfs-integration-test/master-state \
    --port=10011 > dfs-integration-test/logs/master.log 2>&1 &
master_pid=$!

echo "Relaunching Workers."
worker_pids=(0 0 0)
for i in ${!worker_pids[*]}
do
    echo "Launching worker $i"
    ./worker -m "${local_ip}:10011" -i "${local_ip}" --dfs \
        --storage-location="$PWD"/dfs-integration-test/worker"${i}"-state \
        --state-location="$PWD"/dfs-integration-test/worker"${i}"-state > dfs-integration-test/logs/worker-"${i}".log 2>&1 &
    worker_pids[$i]=$!
done

echo ${worker_pids[*]}

sleep 1

# Launch a MapReduce using the above data.
./cli -m "${local_ip}:10011" run \
    -b /binary/word-counter \
    -i /dfs/input/ \
    -o /dfs/output/

# Launch the CLI to monitor status.
echo "Launching CLI."
attempt_counter=0
while true
do
    echo "Checking Status using CLI..."
    status=$(./cli -m "${local_ip}:10011" status 2>&1 | grep 'DONE\|FAILED\|IN_PROGRESS\|IN_QUEUE' -o)
    echo "    - MapReduce Status: " $status
    if [ "$status" == "DONE" ] || [ "$status" == "FAILED" ]; then

        # Download output
        ./cli -m "${local_ip}:10011" download \
        -l "$PWD"/dfs-integration-test/output \
        -r /dfs/output/

        break; 
    fi

    if [ $attempt_counter -gt 15 ]; then
        echo "Error: Unable to reach an expected status after 15 iterations"
        break;
    fi
    sleep 1
    attempt_counter=$(( attempt_counter+1 ))
done


echo "Killing spawned processes"

# Kill any spawned processes.
$(kill -9 ${master_pid} ${worker_pids[*]});

echo "Verifying output"

# Remove newlines from output
cd dfs-integration-test/output
for file in *
do 
    sed ':a;N;$!ba;s/\n//g' "$file" > tmp; mv tmp $file 
done
cd ../..

# Verify that the output is correct.
for key in "${!results[@]}"
do
    count=$(grep -PR --no-filename --only-matching $key+"\":[[:space:]]*\[[[:space:]]*[0-9]*" dfs-integration-test/output/ | grep -o "[0-9]*")
    if [ "${count}" != "${results[${key}]}" ]
    then
        echo "Error. Expected ${key}=${results[${key}]}, but got: $count"
        exit 1
    fi
done

echo "Integration test passed succesfully"

# Cleanup.
rm -rf dfs-integration-test
