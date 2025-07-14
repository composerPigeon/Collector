FROM openjdk:17
WORKDIR /collector
COPY server/build/libs/*.jar app.jar

#EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]