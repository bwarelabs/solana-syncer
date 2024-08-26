#!/bin/bash
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=syncing_%x.log

#if [[ $# -lt 2 ]]; then
#  echo "expected 2 arguments"
#fi

sudo docker build -t solana-syncer .

sudo docker run --rm --name solana-syncer-job \
  -e JVM_ARGS="-Xmx26g" \
  -v ./config.properties:/app/config.properties \
  -v ./solana-tencent-e042e478aa66.json:/app/solana-tencent-e042e478aa66.json \
  solana-syncer -- read-source=bigtable # start-key=$1 end-key=$2
