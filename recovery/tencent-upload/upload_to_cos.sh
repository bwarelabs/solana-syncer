#!/bin/bash

# Check if required parameters are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <SOURCE_DIRECTORY> <BUCKET_ALIAS> <BUCKET_PATH>"
    exit 1
fi

# Normalize SOURCE_DIRECTORY to remove any trailing slash
SOURCE_DIRECTORY=$(realpath -m "$1")   # Local source directory
BUCKET_ALIAS=$2                        # Bucket alias for COS
BUCKET_PATH=$3                         # Path in the bucket to upload the files

# Verify the COSCLI configuration
if ! coscli config show > /dev/null 2>&1; then
    echo "Error: COSCLI configuration not found or invalid."
    exit 1
fi

# Log file for skipped empty files
LOG_FILE="/app/empty_files.log"
echo "Logging skipped files to $LOG_FILE"
> "$LOG_FILE"  # Clear the log file if it exists

# Iterate recursively over each file in the source directory and upload to COS
find "$SOURCE_DIRECTORY" -type f -name "*.seq" | while read -r file; do
    # Check file size (in bytes) and skip if <= 500 KB (500 * 1024 bytes)
    FILE_SIZE=$(stat --printf="%s" "$file")
    if [ "$FILE_SIZE" -le $((500 * 1024)) ]; then
        echo "Skipping $file (size: ${FILE_SIZE} bytes, <= 500 KB)"
        echo "$file" >> "$LOG_FILE"
        continue
    fi

    # Remove the SOURCE_DIRECTORY prefix to maintain relative path in COS
    RELATIVE_PATH="${file#$SOURCE_DIRECTORY/}"
    
    # Set the full destination path in COS, including the bucket alias and path
    DEST_PATH="cos://$BUCKET_ALIAS/$BUCKET_PATH/$RELATIVE_PATH"
    
    echo "Uploading $file to COS at $DEST_PATH"
    
    # Upload file to the specified COS path
    coscli cp "$file" "$DEST_PATH"
    
    # Check if upload was successful
    if [ $? -ne 0 ]; then
        echo "Error: Failed to upload $file to COS."
        exit 1
    fi
done

echo "All valid files uploaded successfully to COS."
echo "Empty files logged to $LOG_FILE."
