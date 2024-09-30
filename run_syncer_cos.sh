#!/bin/bash

sudo docker build -t solana-syncer .

sudo docker run --rm --name solana-syncer-job \
	-e JVM_ARGS="-Xmx8g" \
	-v ./config.properties:/app/config.properties \
	solana-syncer cos-to-hbase --start-hey=$1 --end-hey=$2