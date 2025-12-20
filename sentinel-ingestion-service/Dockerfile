FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Copy parent pom and all modules (required by parent pom)
COPY pom.xml .
COPY sentinel-common ./sentinel-common
COPY sentinel-ingestion-service ./sentinel-ingestion-service
COPY sentinel-consumer-service ./sentinel-consumer-service

# Build the project
RUN mvn clean package -pl sentinel-ingestion-service -am -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app

# Copy the built JAR
COPY --from=build /app/sentinel-ingestion-service/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
