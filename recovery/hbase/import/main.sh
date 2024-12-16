#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <HBASE_IP> <START_BLOCK> <END_BLOCK> <MOUNT_PATH>"
    exit 1
fi

HBASE_IP=$1
START_BLOCK=$2
END_BLOCK=$3
MOUNT_PATH=$4

IMAGE_NAME="solana-import-data-image"
CONTAINER_NAME="solana-import-data-container"

echo "Building the Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" ./hbase/import

if [ $? -ne 0 ]; then
    echo "Failed to build the Docker image. Please check the Dockerfile and try again."
    exit 1
fi

# Check if the container is already running and stop it if necessary
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping existing container..."
    docker stop $CONTAINER_NAME
fi

# Remove the existing container if it exists
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Removing existing container..."
    docker rm $CONTAINER_NAME
fi

echo "Running the download and import container: $IMAGE_NAME in detached mode"
docker run -d \
    --name "$CONTAINER_NAME" \
    -v "$MOUNT_PATH:$MOUNT_PATH" \
    -e HBASE_HOST="$HBASE_IP:9090" \
    -p 50051:50051 \
    "$IMAGE_NAME" "$START_BLOCK" "$END_BLOCK"

# Wait for the container to finish
echo "Waiting for the container $CONTAINER_NAME to exit..."
docker wait "$CONTAINER_NAME"

# Check the exit status of the container
EXIT_CODE=$(docker inspect "$CONTAINER_NAME" --format='{{.State.ExitCode}}')

if [ "$EXIT_CODE" -ne 0 ]; then
    echo "The download and import container exited with errors (exit code: $EXIT_CODE)."
else
    echo "The download and import container exited successfully."
fi
