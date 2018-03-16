#!/bin/bash

# Setup docker images
cargo build --all
make clean-docker
make build-docker-images

nfs_path="/tmp/cerberus-docker"
master_addr="172.30.0.2:8081"

# Setup /tmp/cerberus-docker
rm -rf ${nfs_path}

mkdir ${nfs_path}
mkdir ${nfs_path}/input
mkdir ${nfs_path}/output
mkdir ${nfs_path}/executables

cp "$PWD"/tests/multi-machine/worker-startup.sh ${nfs_path}/executables/worker-startup.sh
cp "$PWD"/target/debug/examples/word-counter ${nfs_path}/executables/word-counter
echo "how much wood would a wood chuck chuck if a wood chuck could chuck wood" \
	> ${nfs_path}/input/sample.txt

# Launch docker containers.
docker-compose -f tests/multi-machine/docker-compose.yml -p cerberus up -d --scale worker=5 --force-recreate

# Validate output.
declare -A results
results+=( [how]=1 [much]=1 [wood]=4 [would]=1 [a]=2 [chuck]=4 [could]=1 [if]=1 )

./target/debug/cli -m "${master_addr}" run \
	-b /executables/word-counter \
	-i /input \
	-o /output \

./target/debug/cli -m "${master_addr}" cluster_status

# Launch the CLI to monitor status.
echo "Launching CLI."
attempt_counter=0
while true
do
    echo "Checking Status using CLI..."
    status=$(./target/debug/cli -m "${master_addr}" status 2>&1 | grep 'DONE\|FAILED\|IN_PROGRESS\|IN_QUEUE' -o)
    echo "    - MapReduce Status: " $status
    if [ "$status" == "DONE" ] || [ "$status" == "FAILED" ]; then break; fi

    if [ $attempt_counter -gt 15 ]; then
        echo "Error: Unable to reach an expected status after 15 iterations"
        break;
    fi
    sleep 1
    attempt_counter=$(( attempt_counter+1 ))
done

# Take down the docker containers
docker-compose -f tests/multi-machine/docker-compose.yml -p cerberus down

# Remove newlines from output
cd "${nfs_path}"/output
for file in *
do 
    sed ':a;N;$!ba;s/\n//g' "$file" > tmp; mv tmp $file 
done
cd ../..

# Verify that the output is correct.
for key in "${!results[@]}"
do
    count=$(grep -PR --no-filename --only-matching $key+"\":[[:space:]]*\[[[:space:]]*[0-9]*" "${nfs_path}"/output/ | grep -o "[0-9]*")
    if [ "${count}" != "${results[${key}]}" ]
    then
        echo "Error. Expected ${key}=${results[${key}]}, but got: $count"
        exit 1
    fi
done

echo "Multi-Machine test passed succesfully"
