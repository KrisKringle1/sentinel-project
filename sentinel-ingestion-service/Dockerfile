FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Copy parent pom and all modules (required by parent pom)
COPY pom.xml .
COPY sentinel-common/pom.xml ./sentinel-common/pom.xml
COPY sentinel-ingestion-service/pom.xml ./sentinel-ingestion-service/pom.xml

RUN mvn dependency:go-offline -B

COPY sentinel-common/src ./sentinel-common/src
COPY sentinel-ingestion-service/src ./sentinel-ingestion-service/src

# Build the project
RUN mvn clean package -pl sentinel-ingestion-service -am -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app

# Create a non-root user and group
RUN groupadd -r sentinel && useradd -r -g sentinel sentinel

# Copy the built JAR
COPY --from=build --chown=sentinel:sentinel /app/sentinel-ingestion-service/target/sentinel-ingestion-service-1.0.0-SNAPSHOT.jar app.jar

USER sentinel
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
