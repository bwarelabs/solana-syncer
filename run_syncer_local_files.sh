#!/bin/bash
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=syncing_%x.log

sudo docker build -t solana-syncer .

sudo docker run --rm --name solana-syncer-job \
	-e JVM_ARGS="-Xmx26g" \
	-v ./config.properties:/app/config.properties \
	solana-syncer read-source=local-files
