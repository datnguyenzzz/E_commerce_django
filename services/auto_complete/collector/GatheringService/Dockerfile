FROM openjdk:17.0.1-oracle
WORKDIR /app/GatheringService
COPY . .
EXPOSE 8080
RUN ./mvnw clean install
ENTRYPOINT ["java", "-jar", "target/GatheringService-0.0.1-SNAPSHOT.jar", "--server.port=8080"]