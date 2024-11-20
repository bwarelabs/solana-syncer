#!/bin/bash

# Check if required parameters are provided
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <SOURCE_DIRECTORY> <COS_CONFIG_PATH> <BUCKET_ALIAS> <BUCKET_PATH>"
    echo "Example: $0 /path/to/exported_files /path/to/.cos.yaml mybucket /test_recovery"
    exit 1
fi

# Parameters
SOURCE_DIRECTORY=$(realpath "$1")    # Absolute path to directory with exported sequence files
COS_CONFIG_PATH=$(realpath "$2")     # Absolute path to the .cos.yaml configuration file
BUCKET_ALIAS=$3                      # Bucket alias defined in .cos.yaml
BUCKET_PATH=$4                       # Path in the bucket to upload the files

# Docker image name
IMAGE_NAME="coscli-uploader"
CONTAINER_NAME="coscli-uploader-container"
MOUNT_PATH="/app/sequencefiles"

# Build the Docker image
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

# Run the Docker container with mounted volumes for files and config
echo "Running the Docker container to upload files to COS..."

docker run \
    --name "$CONTAINER_NAME" \
    -v "$SOURCE_DIRECTORY:$MOUNT_PATH" \
    -v "$COS_CONFIG_PATH:/root/.cos.yaml" \
    "$IMAGE_NAME" "$MOUNT_PATH" "$BUCKET_ALIAS" "$BUCKET_PATH"

