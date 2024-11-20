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

# Iterate recursively over each file in the source directory and upload to COS
find "$SOURCE_DIRECTORY" -type f -name "*.seq" | while read -r file; do
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

echo "All files uploaded successfully to COS."
