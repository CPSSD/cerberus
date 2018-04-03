#!/bin/bash

if [ "$DATA_ABSTRACTION_LAYER" == "" ]; then
	echo "No value was provided for DATA_ABSTRACTION_LAYER (valid options are 'DFS' and 'S3')"
	echo "Set the value of DATA_ABSTRACTION_LAYER in your environment then try again"
	exit 1
fi

base_command="/bin/worker --port=3000 --master=$MASTER_IP:$MASTER_PORT --ip=$WORKER_IP"

case $DATA_ABSTRACTION_LAYER in
	"DFS")
		echo "Launching worker with DFS support"
		${base_command} --dfs
		;;
	"S3")
		echo "Launching worker with S3 support"
		${base_command} --s3 $S3_BUCKET
		;;
	*)
		echo "Incorrect value provided for DATA_ABSTRACTION_LAYER (valid options are 'DFS' and 'S3')"
		exit 1
		;;
esac
