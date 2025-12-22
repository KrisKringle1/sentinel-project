FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Copy parent pom and all modules (required by parent pom)
COPY pom.xml .
COPY sentinel-common/pom.xml ./sentinel-common/pom.xml
COPY sentinel-consumer-service/pom.xml ./sentinel-consumer-service/pom.xml

RUN mvn dependency:go-offline -B

COPY sentinel-common/src ./sentinel-common/src
COPY sentinel-consumer-service/src ./sentinel-consumer-service/src

# Build the project
RUN mvn clean package -pl sentinel-consumer-service -am -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app

# Create a non-root user and group
RUN groupadd -r sentinel && useradd -r -g sentinel sentinel

# Copy the built JAR
COPY --from=build --chown=sentinel:sentinel /app/sentinel-consumer-service/target/sentinel-consumer-service-1.0.0-SNAPSHOT.jar app.jar

USER sentinel
EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]
