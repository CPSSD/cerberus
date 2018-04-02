#!/bin/bash

if [ "$DATA_ABSTRACTION_LAYER" == "" ]; then
	echo "No value was provided for DATA_ABSTRACTION_LAYER (valid options are 'DFS' and 'S3')"
	echo "Set the value of DATA_ABSTRACTION_LAYER in your environment then try again"
	exit 1
fi

base_command="/home/master --port 8081 -d [::]:8082"

case $DATA_ABSTRACTION_LAYER in
	"DFS")
		echo "Launching master with DFS support"
		echo ${base_command} --dfs
		${base_command} --dfs
		;;
	"S3")
		echo "Launching master with S3 support"
		${base_command} --s3 $S3_BUCKET
		;;
	*)
		echo "Incorrect value provided for DATA_ABSTRACTION_LAYER (valid options are 'DFS' and 'S3')"
		exit 1
		;;
esac
