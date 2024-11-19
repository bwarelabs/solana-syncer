#!/bin/bash

IMAGE_NAME="solana-syncer"
CONTAINER_NAME="solana-syncer-job"

sudo docker build -t $IMAGE_NAME .

# check if the container is already running
if [ "$(sudo docker ps -q -f name=$CONTAINER_NAME)" ]; then
	# if the container is running, stop it
	sudo docker stop $CONTAINER_NAME
fi

# remove the container if it exists
if [ "$(sudo docker ps -aq -f name=$CONTAINER_NAME)" ]; then
	sudo docker rm $CONTAINER_NAME
fi

sudo docker run --rm --name $CONTAINER_NAME \
	-e JVM_ARGS="-Xmx26g" \
	-v ./config.properties:/app/config.properties \
	-v /data:/data \
	$IMAGE_NAME geyser-to-cos