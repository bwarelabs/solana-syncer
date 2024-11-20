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

if ! [[ $START_BLOCK =~ ^[0-9]+$ ]]; then
    echo "Error: START_BLOCK must be a decimal number."
    exit 1
fi

if ! [[ $END_BLOCK =~ ^[0-9]+$ ]]; then
    echo "Error: END_BLOCK must be a decimal number."
    exit 1
fi

echo "All inputs are valid."

IMAGE_NAME="solana-recovery"
CONTAINER_NAME="solana-recovery-container"

echo "Building the Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" .

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


echo "Running the Docker container: $IMAGE_NAME"
docker run -d \
    --name "$CONTAINER_NAME" \
    -v "$MOUNT_PATH:$MOUNT_PATH" \
    -p 50051:50051 \
    "$IMAGE_NAME" "$START_BLOCK" "$END_BLOCK"
