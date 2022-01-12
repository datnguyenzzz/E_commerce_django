FROM openjdk:17.0.1-oracle
WORKDIR /ratings/command
COPY . .
RUN ./mvnw clean install 
ENTRYPOINT ["java", "-jar", "target/recommender-0.0.1-SNAPSHOT.jar"]