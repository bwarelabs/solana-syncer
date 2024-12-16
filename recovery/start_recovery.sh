#!/bin/bash

if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker and try again."
    exit 1
fi

function display_help {
    echo "Usage: $0 <START_BLOCK> <END_BLOCK> <MOUNT_PATH> <BUCKET_ALIAS> <BUCKET_PATH>"
    echo
    echo "Arguments:"
    echo "  START_BLOCK    The starting block number (must be a decimal number)."
    echo "  END_BLOCK      The ending block number (must be a decimal number)."
    echo "  MOUNT_PATH     The path where the snapshot archive will be downloaded (must exist)."
    echo "  BUCKET_ALIAS   The bucket alias defined in the .cos.yaml configuration file."
    echo "  BUCKET_PATH    The path in the bucket to upload the files. Example: /test_recovery"
    echo
    echo "Options:"
    echo "  --help         Display this help message and exit."
    exit 0
}

if [ "$1" == "--help" ]; then
    display_help
fi

if [ "$#" -ne 5 ]; then
    echo "Error: Invalid number of arguments."
    echo
    display_help
    exit 1
fi

START_BLOCK=$1
END_BLOCK=$2
MOUNT_PATH=$3
BUCKET_ALIAS=$4
BUCKET_PATH=$5

if ! [[ $START_BLOCK =~ ^[0-9]+$ ]]; then
    echo "Error: START_BLOCK must be a decimal number."
    exit 1
fi

if ! [[ $END_BLOCK =~ ^[0-9]+$ ]]; then
    echo "Error: END_BLOCK must be a decimal number."
    exit 1
fi

if [ ! -d "$MOUNT_PATH" ]; then
    echo "Error: MOUNT_PATH must be a valid directory."
    exit 1
fi

if [ -z "$BUCKET_ALIAS" ]; then
    echo "Error: BUCKET_ALIAS must be a non-empty string."
    exit 1
fi

echo "Starting HBase setup..."
./hbase/setup/main.sh

# Retrieve the HBase container ID
if [ ! -f "./hbase/setup/hbase_container_id.txt" ]; then
    echo "Error: HBase container ID file not found. Ensure HBase is running."
    exit 1
fi

HBASE_CONTAINER_ID=$(cat ./hbase/setup/hbase_container_id.txt)
if [ -z "$HBASE_CONTAINER_ID" ]; then
    echo "Error: HBase container ID is empty. Ensure HBase is running."
    exit 1
fi

# Retrieve the HBase container's IP address
HBASE_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$HBASE_CONTAINER_ID")
if [ -z "$HBASE_IP" ]; then
    echo "Error: Could not retrieve HBase container IP address."
    exit 1
fi

echo "HBase is running with IP: $HBASE_IP"

echo "Calling HBase import script with container ID..."
./hbase/import/main.sh "$HBASE_IP" "$START_BLOCK" "$END_BLOCK" "$MOUNT_PATH"

if [ $? -ne 0 ]; then
    echo "Error: HBase import script failed."
    exit 1
fi

echo "Exporting data from HBase..."
./hbase/export/main.sh "$HBASE_CONTAINER_ID" "$START_BLOCK" "$END_BLOCK" "/sequencefiles"

if [ $? -eq 0 ]; then
    echo "Export process completed successfully."
else
    echo "Export process failed."
    exit 1
fi

echo "Uploading data to COS..."
./tencent-upload/main.sh "./sequencefiles" "./tencent-upload/.cos.yaml" "$BUCKET_ALIAS" "$BUCKET_PATH"

if [ $? -eq 0 ]; then
    echo "Upload process completed successfully."
else
    echo "Upload process failed."
    exit 1
fi

echo "Recovery process completed successfully."
