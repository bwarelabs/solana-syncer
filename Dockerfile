FROM eclipse-temurin:17

RUN apt-get update
RUN apt-get install -y maven

RUN mkdir app
WORKDIR app
COPY pom.xml pom.xml
RUN mvn dependency:resolve

COPY src src
COPY config.properties config.properties
RUN mvn package
