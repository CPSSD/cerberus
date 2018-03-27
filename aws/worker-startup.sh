#!/bin/bash
/bin/worker --port=3000 --master=$MASTER_IP:$MASTER_PORT --ip=$WORKER_IP --dfs
