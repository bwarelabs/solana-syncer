#!/bin/bash

#if [[ $# -lt 2 ]]; then
#  echo "expected 2 arguments"
#fi

sudo docker build -t solana-syncer .

sudo docker run --rm --name solana-syncer-job \
  -e JVM_ARGS="-Xmx26g" \
  -v ./config.properties:/app/config.properties \
  -v ./test.json:/app/test.json \
  solana-syncer -- read-source=bigtable # start-key=$1 end-key=$2
