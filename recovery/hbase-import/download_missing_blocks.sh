#!/bin/bash

# Input: Start and end block numbers
START_BLOCK=$1
END_BLOCK=$2

# Verify input
if [[ -z "$START_BLOCK" || -z "$END_BLOCK" ]]; then
    echo "Usage: $0 <start_block> <end_block>"
    exit 1
fi

# Define the GCS bucket locations
BUCKETS=(
    "gs://mainnet-beta-ledger-us-ny5"
    "gs://mainnet-beta-ledger-europe-fr2"
    "gs://mainnet-beta-ledger-asia-sg1"
)

# Function to list, filter, and sort folders by numeric structure
list_and_sort_buckets() {
    local bucket=$1
    gsutil ls "$bucket" | grep -E "^${bucket}/[0-9]+/$" | sed "s|${bucket}/||" | sed 's|/||' | sort -n
}

# Function to find the closest bucket for a given block
find_closest_bucket() {
    local bucket_list=("$@")
    local target_block=$1
    local closest_bucket=""

    for slot in "${bucket_list[@]:1}"; do
        if (( slot < target_block )); then
            closest_bucket=$slot
        else
            break
        fi
    done

    echo "$closest_bucket"
}

# Main logic to find required buckets and keep track of the specific bucket
declare -A required_buckets  # Associative array to store slot:bucket pairs
found_required_buckets=false

for bucket in "${BUCKETS[@]}"; do
    sorted_buckets=($(list_and_sort_buckets "$bucket"))

    closest_start=$(find_closest_bucket "$START_BLOCK" "${sorted_buckets[@]}")
    closest_end=$(find_closest_bucket "$END_BLOCK" "${sorted_buckets[@]}")

    if [[ -n "$closest_start" && -n "$closest_end" ]]; then
        if (( closest_start == closest_end )); then
            required_buckets["$closest_start"]=$bucket
        else
            for slot in "${sorted_buckets[@]}"; do
                if (( slot >= closest_start && slot <= closest_end )); then
                    required_buckets["$slot"]=$bucket
                fi
            done
        fi
        found_required_buckets=true
    fi

    if [[ "$found_required_buckets" == true ]]; then
        break
    fi
done

echo "Required buckets for slots $START_BLOCK to $END_BLOCK:"
for slot in "${!required_buckets[@]}"; do
    echo "Slot $slot: ${required_buckets[$slot]}/$slot"
done

# Function to download and extract the required archive with progress
download_and_extract_archive() {
    local bucket=$1
    local slot=$2
    local download_dir="/data/recovery/rocksdb/$slot"
    mkdir -p "$download_dir"
    
    local url_base="https://storage.googleapis.com/${bucket#gs://}/$slot"

    echo "Downloading and extracting archive for slot $slot from $bucket"
    # Try downloading zst first with progress indicator
    if wget --show-progress "$url_base/rocksdb.tar.zst" -P "$download_dir"; then
        echo "Downloaded rocksdb.tar.zst from $bucket for slot $slot"
        echo "Extracting rocksdb.tar.zst..."
        pv -f -s $(du -sb "$download_dir/rocksdb.tar.zst" | awk '{print $1}') "$download_dir/rocksdb.tar.zst" | tar --use-compress-program=unzstd -xf - -C "$download_dir"
    elif wget --show-progress "$url_base/rocksdb.tar.bz2" -P "$download_dir"; then
        echo "Downloaded rocksdb.tar.bz2 from $bucket for slot $slot"
        echo "Extracting rocksdb.tar.bz2..."
        pv -f -s $(du -sb "$download_dir/rocksdb.tar.bz2" | awk '{print $1}') "$download_dir/rocksdb.tar.bz2" | tar -I lbzip2 -xf - -C "$download_dir"
    else
        echo "Failed to download rocksdb archive from $bucket for slot $slot"
        return 1
    fi

    if curl -# -o "$download_dir/version.txt" "$url_base/version.txt"; then
        echo "Downloaded version.txt for slot $slot"
        cat "$download_dir/version.txt"
    else
        echo "Failed to download version.txt for slot $slot"
    fi

    if [[ -d "$download_dir/rocksdb" ]]; then
        echo "Successfully extracted ledger data for slot $slot"
    else
        echo "Extraction failed for slot $slot"
        return 1
    fi
}

# Download and extract archives for each required slot from the specific bucket
for slot in "${!required_buckets[@]}"; do
    bucket="${required_buckets[$slot]}"
    download_and_extract_archive "$bucket" "$slot"
done
