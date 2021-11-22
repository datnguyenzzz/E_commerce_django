FROM openjdk:17.0.1-oracle
ADD target/GatheringService-0.0.1-SNAPSHOT.jar /app/GatheringService/server.jar
WORKDIR /app/GatheringService 
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "server.jar", "--server.port=8080"]