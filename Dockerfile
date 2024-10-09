FROM rust:1.80.1-bookworm as rust-builder
WORKDIR /app
COPY solana-bigtable/Cargo.lock Cargo.lock
COPY solana-bigtable/Cargo.toml Cargo.toml
# this is a small hack to allow building the dependencies first
# so incremental builds are faster
RUN mkdir src \
    && echo "// dummy file" > src/lib.rs \
    && cargo build --release

COPY solana-bigtable/src src

RUN sh -c "echo '// updated' >> src/lib.rs && cargo build --release"

FROM eclipse-temurin:22

RUN apt-get update
RUN apt-get install -y maven vim

RUN mkdir app
WORKDIR /app
COPY pom.xml pom.xml
RUN mvn dependency:resolve

COPY --from=rust-builder /app/target/release/libsolana_bigtable.so /usr/lib/libsolana_bigtable.so

COPY src src
RUN mvn package

COPY start.sh start.sh

ENTRYPOINT ["/app/start.sh"]
