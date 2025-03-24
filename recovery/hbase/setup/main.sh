#!/bin/bash

IMAGE_NAME="hbase-image"
CONTAINER_NAME="hbase-container"

echo "Building HBase Docker image..."
docker build -t "$IMAGE_NAME" ./hbase/setup

if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "There is already a running HBase container. This is a safety measure to prevent data loss."
    echo "If you are sure you want rebuild and recreate a new HBase container, please remove this check."
    echo "The check is located in:"
    pwd
    echi "Exiting..."
    exit 1
fi

if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping running HBase container..."
    docker stop "$CONTAINER_NAME"
fi

if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Removing existing HBase container..."
    docker rm "$CONTAINER_NAME"
fi

echo "HBase Docker image built successfully."
echo "Starting HBase container..."
mkdir -p ./sequencefiles
# mkdir -p /data/hbase
# -v /data/hbase:/hbase-data \

HBASE_CONTAINER_ID=$(
    docker run -d \
        --name "$CONTAINER_NAME" \
        -v "./hbase/export/export_sequencefiles.sh:/export_sequencefiles.sh" \
        -v "./sequencefiles:/sequencefiles" \
        -v "/tmp:/tmp" \
        -p 16010:16010 \
        -p 16020:16020 \
        -p 16030:16030 \
        -p 9090:9090 \
        "$IMAGE_NAME"
)

echo "Waiting for HBase readiness..."
while [ ! -f /tmp/hbase_ready ]; do sleep 5; done

echo "HBase is up and running."
rm /tmp/hbase_ready

# Output the container ID for use by other scripts
echo "HBase container ID: $HBASE_CONTAINER_ID"
echo "$HBASE_CONTAINER_ID" > ./hbase/setup/hbase_container_id.txt