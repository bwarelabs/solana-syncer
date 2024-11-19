# Stage 1: Build the Rust binaries
FROM rust:latest AS builder

# Install dependencies needed for building
RUN apt-get update && apt-get install -y \
    cmake \
    protobuf-compiler \
    clang \
    pkg-config \
    libssl-dev \
    libudev-dev \
    build-essential \
    git

# Clone and build agave/ledger-tool
WORKDIR /usr/src/agave
RUN git clone https://github.com/anza-xyz/agave.git . && \
    cd ledger-tool && \
    cargo build --release

# Clone and build solana-bigtable-hbase-adapter
WORKDIR /usr/src/solana-bigtable-hbase-adapter
RUN git clone https://github.com/bwarelabs/solana-bigtable-hbase-adapter.git . && \
    cargo build --release

# Stage 2: Prepare runtime image with Ubuntu
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    gnupg \
    curl \
    protobuf-compiler \
    libssl-dev \
    netcat \
    wget \
    pv \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# Add Google Cloud SDK for gsutil
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update -y && \
    apt-get install google-cloud-cli -y

# Copy the built binaries from the builder stage
COPY --from=builder /usr/src/agave/target/release/agave-ledger-tool /usr/recovery/agave-ledger-tool
COPY --from=builder /usr/src/solana-bigtable-hbase-adapter/target/release/server /usr/recovery/solana-bigtable-hbase-adapter

# Copy scripts and make them executable
WORKDIR /usr/recovery
COPY download_missing_blocks.sh /usr/recovery/download_missing_blocks.sh
COPY entrypoint.sh /usr/recovery/entrypoint.sh
RUN chmod +x /usr/recovery/download_missing_blocks.sh /usr/recovery/entrypoint.sh

ENV BIGTABLE_EMULATOR_HOST="localhost:50051"
ENV HBASE_HOST="localhost:9090"

# Define entrypoint
ENTRYPOINT ["/usr/recovery/entrypoint.sh"]
