FROM openjdk:17.0.1-oracle 
WORKDIR /recommender/command-rating-service
COPY target/command-rating-service-1.0-SNAPSHOT.jar .
EXPOSE 8000
ENTRYPOINT ["java","-Xmx256m","-Xms256m","-jar",\
            "command-rating-service-1.0-SNAPSHOT.jar"]