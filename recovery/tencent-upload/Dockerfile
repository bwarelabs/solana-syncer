# Use a lightweight Linux base image
FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Download and install COSCLI
RUN wget -O /usr/local/bin/coscli https://cosbrowser.cloud.tencent.com/software/coscli/coscli-linux-amd64 \
    && chmod +x /usr/local/bin/coscli

# Verify installation
RUN coscli --version

# Set the working directory
WORKDIR /app

# Copy the .cos.yaml configuration file into the container (placeholder if testing)
# COPY .cos.yaml /root/.cos.yaml

# Copy the script that uploads files to COS
COPY upload_to_cos.sh /app/upload_to_cos.sh
RUN chmod +x /app/upload_to_cos.sh

# Set the entrypoint to the upload script
ENTRYPOINT ["/app/upload_to_cos.sh"]
# CMD ["sleep", "infinity"]

