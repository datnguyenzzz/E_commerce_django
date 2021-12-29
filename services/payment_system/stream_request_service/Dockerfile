FROM openjdk:8-jdk
RUN apt-get update 
WORKDIR /client_api_service
COPY . .
RUN ./gradlew clean build 