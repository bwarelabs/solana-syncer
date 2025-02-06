#!/bin/bash

echo "If you are using this in a cluster, add your DNS mappings to the --add-host arguments"

# Usage check
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <start-key-decimal> <end-key-decimal>"
  exit 1
fi

sudo docker build -t solana-syncer .

#  Add your infrastructure ip addresses to the --add-host arguments
#   --add-host=hbase-worker-0:172.16.1.16 \
#   --add-host=hbase-worker-1:172.16.2.12 \
#   --add-host=solana:172.16.1.8 \

sudo docker run --rm -it --name solana-syncer-job \
  --add-host=hbase-worker-0:172.16.1.16 \
  --add-host=hbase-worker-1:172.16.2.12 \
  --add-host=solana:172.16.1.8 \
  --network=host \
  -e JVM_ARGS="-Xmx8g" \
  -v ./config.properties:/app/config.properties \
  solana-syncer cos-to-hbase  --start-key="$1" --end-key="$2"