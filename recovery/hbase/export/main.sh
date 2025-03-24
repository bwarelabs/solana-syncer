#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <HBASE_CONTAINER_ID> <START_BLOCK> <END_BLOCK> <OUTPUT_PATH>"
    exit 1
fi

HBASE_CONTAINER_ID=$1
START_BLOCK=$2
END_BLOCK=$3
OUTPUT_PATH=$4

# Validate START_BLOCK and END_BLOCK
if ! [[ $START_BLOCK =~ ^[0-9]+$ ]]; then
    echo "Error: START_BLOCK must be a decimal number."
    exit 1
fi

if ! [[ $END_BLOCK =~ ^[0-9]+$ ]]; then
    echo "Error: END_BLOCK must be a decimal number."
    exit 1
fi

echo "Triggering export script inside HBase container ID: $HBASE_CONTAINER_ID"

# Execute the export script for 'blocks' table
echo "Exporting data for table 'blocks'..."
docker exec "$HBASE_CONTAINER_ID" /bin/bash /export_sequencefiles.sh "blocks" "$START_BLOCK" "$END_BLOCK" "$OUTPUT_PATH"

if [ $? -ne 0 ]; then
    echo "Error: Export failed for table 'blocks'."
    exit 1
fi

# Execute the export script for 'entries' table
echo "Exporting data for table 'entries'..."
docker exec "$HBASE_CONTAINER_ID" /bin/bash /export_sequencefiles.sh "entries" "$START_BLOCK" "$END_BLOCK" "$OUTPUT_PATH"

if [ $? -ne 0 ]; then
    echo "Error: Export failed for table 'entries'."
    exit 1
fi

echo "Data export completed successfully for both tables."
