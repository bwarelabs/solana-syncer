FROM eclipse-temurin:17

RUN apt-get update
RUN apt-get install -y maven vim

RUN mkdir app
WORKDIR app
COPY pom.xml pom.xml
RUN mvn dependency:resolve

COPY src src
COPY config.properties config.properties
COPY solana-tencent-e042e478aa66.json solana-tencent-e042e478aa66.json
RUN mvn package
