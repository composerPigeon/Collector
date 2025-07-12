FROM openjdk:17
WORKDIR /collector
CMD ["./gradlew", "clean", "bootJar"]
COPY server/build/libs/*.jar app.jar

#EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]