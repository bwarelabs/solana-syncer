#!/bin/bash

sudo docker build -t solana-syncer .

sudo docker run --rm --name solana-syncer-job \
  -e JVM_ARGS="-Xmx26g" \
  -v ./config.properties:/app/config.properties \
  -v ./test.json:/app/test.json \
  solana-syncer bigtable-to-cos --start-key=$1 --end-key=$2
