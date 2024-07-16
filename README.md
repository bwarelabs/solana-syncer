# Syncer

The Syncer service is designed to facilitate data synchronization from 2 sources to Tencent Cloud Object Storage (COS). It supports reading data from Google BigTable and local files, converting this data into sequence files, and uploading these files to COS using the AWS S3 SDK.

## Overview
### Features
1. **BigTable Integration**:
    - Reads data from specified BigTable tables.
    - Supports multiple table such as blocks, entries, transactions, and transactions by address.
    - Handles large datasets by splitting them into manageable chunks for processing and uploading.

2. **Local File Integration**:
   - Monitors a specified local directory for new data.
   - Processes files in various formats, converting them into sequence files for consistency.
   - Automatically uploads processed files to COS.

3. **COS Upload**:
   - Utilizes the AWS S3 SDK to upload sequence files to Tencent Cloud Object Storage.
   - Ensures data integrity and efficient transfer through configurable batch processing.

### Configuration

The Syncer relies on a configuration file (`config.properties`) to manage settings such as BigTable connection details, local file directories, COS credentials, and processing parameters. This allows for flexible and scalable deployments.

## How to run the Syncer
* exec into the container
`docker compose exec syncer /bin/bash` (check this repo for more details: https://github.com/bwarelabs/solana-test-setup)

* start the process  
`java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/syncer-1.0-SNAPSHOT.jar read-source=bigtable`   
`java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/syncer-1.0-SNAPSHOT.jar read-source=local-files`  

* it writes each data into `./output/sequencefiles/{table_name}` directory

## Environment Variables
This container uses the following environment variables:
1. **BIGTABLE_EMULATOR_HOST**: Points to the BigTable emulator service for integration.