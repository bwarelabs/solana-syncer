#!/bin/bash

# Check if required parameters are provided
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <TABLE_NAME> <START_KEY> <END_KEY> <OUTPUT_PATH>"
    exit 1
fi

TABLE_NAME=$1
START_KEY_HEX=$2
END_KEY_HEX=$3
OUTPUT_PATH=$4
ROWS_PER_EXPORT=1000

# Convert START_KEY and END_KEY from hex to decimal for calculation
START_KEY_DEC=$((16#$START_KEY_HEX))
END_KEY_DEC=$((16#$END_KEY_HEX))

# Loop until START_KEY_DEC reaches END_KEY_DEC
CURRENT_START_DEC=$START_KEY_DEC
while [ "$CURRENT_START_DEC" -lt "$END_KEY_DEC" ]; do
    # Calculate the next stop key in decimal
    CURRENT_STOP_DEC=$((CURRENT_START_DEC + ROWS_PER_EXPORT))
    if [ "$CURRENT_STOP_DEC" -gt "$END_KEY_DEC" ]; then
        CURRENT_STOP_DEC=$END_KEY_DEC
    fi

    # Convert start and stop keys back to hex
    CURRENT_START_HEX=$(printf "%016X" "$CURRENT_START_DEC")
    CURRENT_STOP_HEX=$(printf "%016X" "$CURRENT_STOP_DEC")

    # Define output directory for this export, e.g., /output_path/table_name/range_start_stop
    EXPORT_DIR="${OUTPUT_PATH}/${TABLE_NAME}/range_${CURRENT_START_HEX}_${CURRENT_STOP_HEX}"

    # Export range using HBase Export tool
    echo "Exporting rows from $CURRENT_START_HEX to $CURRENT_STOP_HEX"
    hbase org.apache.hadoop.hbase.mapreduce.Export \
        -D hbase.mapreduce.scan.row.start="$CURRENT_START_HEX" \
        -D hbase.mapreduce.scan.row.stop="$CURRENT_STOP_HEX" \
        "$TABLE_NAME" "$EXPORT_DIR"

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

    # Update CURRENT_START_DEC for the next range
    CURRENT_START_DEC=$CURRENT_STOP_DEC
done

echo "Export completed for all ranges from $START_KEY_HEX to $END_KEY_HEX in table $TABLE_NAME."
