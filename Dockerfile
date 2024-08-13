FROM eclipse-temurin:22

RUN apt-get update
RUN apt-get install -y maven vim

RUN mkdir app
WORKDIR app
COPY pom.xml pom.xml
RUN mvn dependency:resolve

COPY src src
RUN mvn package

COPY start.sh start.sh

ENTRYPOINT ["/app/start.sh"]
