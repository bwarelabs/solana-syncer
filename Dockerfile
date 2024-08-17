FROM rust:1.80.1-bookworm as rust-builder
WORKDIR /app
COPY solana-bigtable/Cargo.lock Cargo.lock
COPY solana-bigtable/Cargo.toml Cargo.toml
RUN mkdir src \
    && echo "// dummy file" > src/lib.rs \
    && cargo build --release

RUN rm -rf src
COPY solana-bigtable/src src

RUN cargo build --release

FROM eclipse-temurin:22

RUN apt-get update
RUN apt-get install -y maven vim

RUN mkdir app
WORKDIR app
COPY pom.xml pom.xml
RUN mvn dependency:resolve

COPY --from=rust-builder /app/target/release/libsolana_bigtable.so /usr/lib/libsolana_bigtable.so

COPY src src
RUN mvn package

COPY start.sh start.sh

ENTRYPOINT ["/app/start.sh"]
