#!/bin/bash

if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker and try again."
    exit 1
fi

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <START_BLOCK> <END_BLOCK> <MOUNT_PATH>"
    exit 1
fi

START_BLOCK=$1
END_BLOCK=$2
MOUNT_PATH=$3

IMAGE_NAME="solana-recovery"

echo "Building the Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" .

if [ $? -ne 0 ]; then
    echo "Failed to build the Docker image. Please check the Dockerfile and try again."
    exit 1
fi

echo "Running the Docker container: $IMAGE_NAME"
# docker run -v "$MOUNT_PATH" "$IMAGE_NAME" "$START_BLOCK" "$END_BLOCK"

docker run -d \
 --name "solana-recovery-container" \
 -v "$MOUNT_PATH:$MOUNT_PATH" \
 -v "$HOME/solana-diverse/rocksdb.tar.zst:/usr/recovery/rocksdb/295403492/rocksdb.tar.zst:ro" \
 -v "/data/rocksdb_extracted/rocksdb:/usr/recovery/rocksdb/295403492/rocksdb" \
 "$IMAGE_NAME" "$START_BLOCK" "$END_BLOCK"


