#!/bin/bash

# Usage check
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <start-key> <end-key>"
  exit 1
fi

sudo docker build -t solana-syncer .

sudo docker run --rm --name solana-syncer-job \
  -e JVM_ARGS="-Xmx26g" \
  -v ./config.properties:/app/config.properties \
  -v ./test.json:/app/test.json \
  solana-syncer bigtable-to-cos --start-key=$1 --end-key=$2
