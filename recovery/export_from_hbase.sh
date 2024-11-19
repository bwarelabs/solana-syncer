#!/bin/bash

# Check if required parameters are provided
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <TABLE_NAME> <START_KEY> <END_KEY> <OUTPUT_PATH>"
    exit 1
fi

TABLE_NAME=$1
START_KEY=$2
END_KEY=$3
OUTPUT_PATH=$4
ROWS_PER_EXPORT=1000

if ! [[ $START_KEY =~ ^[0-9]+$ ]]; then
    echo "Error: START_KEY must be a decimal number."
    exit 1
fi

if ! [[ $END_KEY =~ ^[0-9]+$ ]]; then
    echo "Error: END_KEY must be a decimal number."
    exit 1
fi

echo "All inputs are valid."

# Loop until START_KEY reaches END_KEY_DEC
CURRENT_START=$START_KEY
while [ "$CURRENT_START" -lt "$END_KEY" ]; do
    # Calculate the next stop key in decimal
    CURRENT_STOP=$((CURRENT_START + ROWS_PER_EXPORT))
    if [ "$CURRENT_STOP" -gt "$END_KEY" ]; then
        CURRENT_STOP=$END_KEY
    fi

    # Convert start and stop keys back to hex and ensure they are lowercase
    CURRENT_START_HEX=$(printf "%016X" "$CURRENT_START" | tr 'A-F' 'a-f')
    CURRENT_STOP_HEX=$(printf "%016X" "$CURRENT_STOP" | tr 'A-F' 'a-f')


    # Define output directory for this export, e.g., /output_path/table_name/range_start_stop
    EXPORT_DIR="${OUTPUT_PATH}/${TABLE_NAME}/range_${CURRENT_START_HEX}_${CURRENT_STOP_HEX}"

    # Export range using HBase Export tool
    echo "Exporting rows from $CURRENT_START_HEX to $CURRENT_STOP_HEX"
    hbase org.apache.hadoop.hbase.mapreduce.Export \
        -D hbase.mapreduce.scan.row.start="$CURRENT_START_HEX" \
        -D hbase.mapreduce.scan.row.stop="$CURRENT_STOP_HEX" \
        -D mapreduce.input.fileinputformat.split.minsize=536870912000 \
        "$TABLE_NAME" "$EXPORT_DIR"
    # 500GB min split size so there are less chances of multiple part-m-0000x files

    # Check for multiple part-m-0000x files
    PART_FILES=("$EXPORT_DIR"/part-m-0000*)
    if [ ${#PART_FILES[@]} -gt 1 ]; then
        echo "Error: Multiple part-m-0000x files found in $EXPORT_DIR. Stopping script."
        exit 1
    fi

    # Check for required files before renaming
    if [[ -f "$EXPORT_DIR/part-m-00000" && -f "$EXPORT_DIR/.part-m-00000.crc" && -f "$EXPORT_DIR/_SUCCESS" && -f "$EXPORT_DIR/._SUCCESS.crc" ]]; then
        # Rename the output file to blocks.seq
        mv "$EXPORT_DIR/part-m-00000" "$EXPORT_DIR/$TABLE_NAME.seq"
        
        # Delete the other files
        rm "$EXPORT_DIR/.part-m-00000.crc" "$EXPORT_DIR/_SUCCESS" "$EXPORT_DIR/._SUCCESS.crc"
    else
        echo "Error: Required files not found in $EXPORT_DIR. Stopping script."
        exit 1
    fi

    # Update CURRENT_START for the next range
    CURRENT_START=$CURRENT_STOP

done

echo "Export completed for all ranges from $START_KEY to $END_KEY in table $TABLE_NAME."
