#!/bin/bash

set -e

cd ./target/debug

# Setup directories.
mkdir -p distributed-grep-test/logs
mkdir -p distributed-grep-test/input
mkdir -p distributed-grep-test/output

# Launch the master.
echo "Launching Master."
./master --fresh --nodump --port=10008 > distributed-grep-test/logs/master.log 2>&1 &
master_pid=$!

local_ip="127.0.0.1"

# Spin up 3 workers.
worker_pids=(0 0 0)
for i in ${!worker_pids[*]}
do
    echo "Launching worker $i"
    ./worker -m "${local_ip}:10008" -i "${local_ip}" --fresh --nodump > distributed-grep-test/logs/worker-"${i}".log 2>&1 &
    worker_pids[$i]=$!
done

echo ${worker_pids[*]}

# Create some sample data.
echo "Disestablishmentarianism refers to campaigns to sever links between church and state
The word antidisestablishmentarianism is notable for its unusual length of 28 letters
Congratulations is a fifteen letter word" \
    > distributed-grep-test/input/sample.txt

declare -A results
results+=( ["15"]="Congratulations" ["24"]="Disestablishmentarianism" ["28"]="antidisestablishmentarianism" )

# Sleep to allow the master time to startup.
sleep 1

# Launch a MapReduce using the above data.
./cli -m "${local_ip}:10008" run \
    -b "$PWD"/examples/distributed-grep \
    -i "$PWD"/distributed-grep-test/input \
    -o "$PWD"/distributed-grep-test/output

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
    attempt_counter=$(( attempt_counter+1 ))
done

# Kill any spawned processes.
$(kill -9 ${master_pid} ${worker_pids[*]});

# Remove newlines from output
cd distributed-grep-test/output
for file in *
do
    sed ':a;N;$!ba;s/\n//g' "$file" > tmp; mv tmp $file
done
cd ../..

# Verify that the output is correct.
for key in "${!results[@]}"
do
    value=$(grep -PR --no-filename --only-matching $key+"\":[[:space:]]*\[([^]]+)\]" distributed-grep-test/output/)
    if [[ $value != *${results[${key}]}* ]]; then
        echo "Did not find expected word ${results[${key}]} in $value"
        exit 1
    fi
done

echo "Distributed grep test passed succesfully"

# Cleanup.
rm -rf distributed-grep-test
