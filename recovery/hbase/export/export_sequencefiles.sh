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

check_multiple_regions() {
  # Table name passed as an argument
  local TABLE_NAME=$1

  if [[ -z "$TABLE_NAME" ]]; then
    echo "Error: Table name is required."
    return 1
  fi

  echo "Checking regions for table: $TABLE_NAME"

  # Fetch the region count directly from the "x rows" line
  echo "Fetching region count for table: $TABLE_NAME"
  REGION_OUTPUT=$(echo "list_regions '$TABLE_NAME'" | hbase shell)

  # Extract the number of rows (regions)
  NUM_REGIONS=$(echo "$REGION_OUTPUT" | grep -oP '\d+(?= rows)' | head -n 1)

  if [[ -z "$NUM_REGIONS" ]]; then
    echo "Error: Could not determine the number of regions for table $TABLE_NAME."
    return 1
  fi

  # Debug: Print the number of regions detected
  echo "Number of regions detected: $NUM_REGIONS"

  if [[ $NUM_REGIONS -eq 1 ]]; then
    echo "Table $TABLE_NAME has exactly one region. Exporting..."
    return 0
  else
    echo "Error: Table $TABLE_NAME does not have exactly one region ($NUM_REGIONS detected)."
    return 1
  fi
}

export_sequencefile () {
    # Table name passed as an argument
    local TABLE_NAME=$1
    local START_KEY=$2
    local END_KEY=$3
    local OUTPUT_PATH=$4

    # Calculate the nearest multiple of 1000 greater than START_KEY
    NEAREST_MULTIPLE=$(( (START_KEY + ROWS_PER_EXPORT - 1) / ROWS_PER_EXPORT * ROWS_PER_EXPORT ))

    # Export skipped range if START_KEY is not already a multiple of 1000
    if [ "$START_KEY" -lt "$NEAREST_MULTIPLE" ]; then
        SKIPPED_RANGE_DIR="${OUTPUT_PATH}/${TABLE_NAME}/range_${START_KEY}_${NEAREST_MULTIPLE}"
        echo "Exporting skipped range from $START_KEY to $NEAREST_MULTIPLE"
        hbase org.apache.hadoop.hbase.mapreduce.Export \
            -D hbase.mapreduce.scan.row.start=$(printf "%016X" "$START_KEY" | tr 'A-F' 'a-f') \
            -D hbase.mapreduce.scan.row.stop=$(printf "%016X" "$NEAREST_MULTIPLE" | tr 'A-F' 'a-f') \
            -D mapreduce.input.fileinputformat.split.minsize=536870912000 \
            "$TABLE_NAME" "$SKIPPED_RANGE_DIR"

        # Perform file checks and renaming for the skipped range
        PART_FILES=("$SKIPPED_RANGE_DIR"/part-m-0000*)
        if [ ${#PART_FILES[@]} -gt 1 ]; then
            echo "Error: Multiple part-m-0000x files found in $SKIPPED_RANGE_DIR. Stopping script."
            exit 1
        fi

        if [[ -f "$SKIPPED_RANGE_DIR/part-m-00000" && -f "$SKIPPED_RANGE_DIR/.part-m-00000.crc" && -f "$SKIPPED_RANGE_DIR/_SUCCESS" && -f "$SKIPPED_RANGE_DIR/._SUCCESS.crc" ]]; then
            mv "$SKIPPED_RANGE_DIR/part-m-00000" "$SKIPPED_RANGE_DIR/$TABLE_NAME.seq"
            rm "$SKIPPED_RANGE_DIR/.part-m-00000.crc" "$SKIPPED_RANGE_DIR/_SUCCESS" "$SKIPPED_RANGE_DIR/._SUCCESS.crc"
        else
            echo "Error: Required files not found in $SKIPPED_RANGE_DIR. Stopping script."
            exit 1
        fi

        # Update START_KEY to the nearest multiple
        START_KEY=$NEAREST_MULTIPLE
    fi

    # Loop until START_KEY reaches END_KEY
    CURRENT_START=$START_KEY
    while [ "$CURRENT_START" -lt "$END_KEY" ]; do
        # Calculate the next stop key in decimal
        CURRENT_STOP=$((CURRENT_START + ROWS_PER_EXPORT))
        if [ "$CURRENT_STOP" -gt "$END_KEY" ]; then
            CURRENT_STOP=$END_KEY
        fi

        # Convert start and stop keys to hex and ensure they are lowercase
        CURRENT_START_HEX=$(printf "%016X" "$CURRENT_START" | tr 'A-F' 'a-f')
        CURRENT_STOP_HEX=$(printf "%016X" "$CURRENT_STOP" | tr 'A-F' 'a-f')

        # Define output directory for this export
        EXPORT_DIR="${OUTPUT_PATH}/${TABLE_NAME}/range_${CURRENT_START_HEX}_${CURRENT_STOP_HEX}"

        # Export range using HBase Export tool
        echo "Exporting rows from $CURRENT_START_HEX to $CURRENT_STOP_HEX"
        hbase org.apache.hadoop.hbase.mapreduce.Export \
            -D hbase.mapreduce.scan.row.start="$CURRENT_START_HEX" \
            -D hbase.mapreduce.scan.row.stop="$CURRENT_STOP_HEX" \
            -D mapreduce.input.fileinputformat.split.minsize=536870912000 \
            "$TABLE_NAME" "$EXPORT_DIR"

        # Perform file checks and renaming
        PART_FILES=("$EXPORT_DIR"/part-m-0000*)
        if [ ${#PART_FILES[@]} -gt 1 ]; then
            echo "Error: Multiple part-m-0000x files found in $EXPORT_DIR. Stopping script."
            exit 1
        fi

        if [[ -f "$EXPORT_DIR/part-m-00000" && -f "$EXPORT_DIR/.part-m-00000.crc" && -f "$EXPORT_DIR/_SUCCESS" && -f "$EXPORT_DIR/._SUCCESS.crc" ]]; then
            mv "$EXPORT_DIR/part-m-00000" "$EXPORT_DIR/$TABLE_NAME.seq"
            rm "$EXPORT_DIR/.part-m-00000.crc" "$EXPORT_DIR/_SUCCESS" "$EXPORT_DIR/._SUCCESS.crc"
        else
            echo "Error: Required files not found in $EXPORT_DIR. Stopping script."
            exit 1
        fi

        # Update CURRENT_START for the next range
        CURRENT_START=$CURRENT_STOP

    done
}


# Check if the table has multiple regions
check_multiple_regions $TABLE_NAME

if [ $? -ne 0 ]; then
    echo "Error: Table $TABLE_NAME has multiple regions. Exporting is not supported."
    exit 1
fi

# Export sequence files for the specified range
export_sequencefile $TABLE_NAME $START_KEY $END_KEY $OUTPUT_PATH
echo "Export completed for all ranges from $START_KEY to $END_KEY in table $TABLE_NAME."