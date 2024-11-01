#!/bin/bash

# Input parameters for agave-ledger-tool
START_BLOCK=$1
END_BLOCK=$2

# Check if START_BLOCK and END_BLOCK are provided
if [[ -z "$START_BLOCK" || -z "$END_BLOCK" ]]; then
    echo "Usage: <start_block> <end_block>"
    exit 1
fi

# Run download_missing_blocks.sh and wait for it to complete
echo "Starting download_missing_blocks.sh..."
/usr/recovery/download_missing_blocks.sh "$START_BLOCK" "$END_BLOCK"

# Check if download_missing_blocks.sh completed successfully
if [[ $? -ne 0 ]]; then
    echo "download_missing_blocks.sh failed. Exiting..."
    exit 1
fi

# Run solana-bigtable-hbase-adapter
echo "Running solana-bigtable-hbase-adapter..."
/usr/recovery/solana-bigtable-hbase-adapter > ./solana-bigtable-hbase-adapter.log 2>&1 &

# Wait until solana-bigtable-hbase-adapter is ready
until nc -z localhost 50051; do
    sleep 1
done
echo "solana-bigtable-hbase-adapter started."

# Iterate over each RocksDB folder and run agave-ledger-tool sequentially
for slot in /usr/recovery/rocksdb/*; do
    if [[ -d "$slot" ]]; then
        echo "Processing RocksDB folder: $slot"
        /usr/recovery/agave-ledger-tool bigtable upload "$START_BLOCK" "$END_BLOCK" -l "$slot"
        
        # Check if agave-ledger-tool succeeded
        if [[ $? -ne 0 ]]; then
            echo "agave-ledger-tool failed for $slot. Exiting..."
            exit 1
        fi
    else
        echo "$slot is not a directory. Skipping..."
    fi
done

echo "All RocksDB folders processed successfully."

# Keep the container running until all background processes finish
wait
