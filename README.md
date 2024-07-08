# Syncer

This folder contains a Docker-based setup for the Syncer service. The syncer reads data from the BigTable emulator and writes it to sequencefiles locally.

## How to run the Syncer

* exec into the container
`docker compose exec syncer /bin/bash`

* start the process  
`java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/syncer-1.0-SNAPSHOT.jar read-source=bigtable`  
`java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/syncer-1.0-SNAPSHOT.jar read-source=local-files`  
`java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/syncer-1.0-SNAPSHOT.jar run-checksum-on-hbase`

* it writes each data into `./output/sequencefiles/{table_name}` directory

## Environment Variables
This container uses the following environment variables:
1. **BIGTABLE_EMULATOR_HOST**: Points to the BigTable emulator service for integration.