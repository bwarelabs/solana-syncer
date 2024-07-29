FROM eclipse-temurin:17

RUN apt-get update
RUN apt-get install -y maven vim

RUN mkdir app
WORKDIR app
COPY pom.xml pom.xml
RUN mvn dependency:resolve

COPY src src
RUN mvn package
