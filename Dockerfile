FROM maven:3.8.4-eclipse-temurin-17 AS BUILDER
COPY pom.xml /tmp/
RUN mvn -B dependency:go-offline -f /tmp/pom.xml -s /usr/share/maven/ref/settings-docker.xml
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn -B -s /usr/share/maven/ref/settings-docker.xml package

FROM arm64v8/openjdk:17-buster

RUN mkdir /app

COPY --from=BUILDER /tmp/target/lib /app/lib
COPY --from=BUILDER /tmp/target/*.jar /app/app.jar

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app/app.jar"]