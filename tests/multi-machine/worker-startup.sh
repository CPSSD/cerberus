#!/bin/bash

hostname="`hostname -I | awk '{ print $1 }'`";
echo "Hostname=${hostname}"

worker --port=3000 --master=172.30.0.2:8081 --nfs=/mnt/nfs/cerberus --ip=${hostname}
