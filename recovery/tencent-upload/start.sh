#!/bin/bash

# Check if required parameters are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <SOURCE_DIRECTORY> <COS_CONFIG_PATH> <BUCKET_ALIAS>"
    echo "Example: $0 /path/to/exported_files /path/to/.cos.yaml mybucket"
    exit 1
fi

# Parameters
SOURCE_DIRECTORY=$(realpath "$1")    # Absolute path to directory with exported sequence files
COS_CONFIG_PATH=$(realpath "$2")     # Absolute path to the .cos.yaml configuration file
BUCKET_ALIAS=$3                      # Bucket alias defined in .cos.yaml

# Docker image name
IMAGE_NAME="coscli-uploader"
MOUNT_PATH="/app/sequencefiles"

# Build the Docker image
echo "Building the Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" .

if [ $? -ne 0 ]; then
    echo "Failed to build the Docker image. Please check the Dockerfile and try again."
    exit 1
fi

echo $SOURCE_DIRECTORY 

# Run the Docker container with mounted volumes for files and config
echo "Running the Docker container to upload files to COS..."

# docker run -d \
#     --name "coscli-uploader-container" \
#     -v "$SOURCE_DIRECTORY:$MOUNT_PATH" \
#     -v "$COS_CONFIG_PATH:/root/.cos.yaml" \
#     "$IMAGE_NAME" 

docker run \
    --name "coscli-uploader-container" \
    -v "$SOURCE_DIRECTORY:$MOUNT_PATH" \
    -v "$COS_CONFIG_PATH:/root/.cos.yaml" \
    "$IMAGE_NAME" "$MOUNT_PATH" "$BUCKET_ALIAS"

