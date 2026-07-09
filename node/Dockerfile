FROM eclipse-temurin:17-jre
WORKDIR /app
# Copy the built jar into the image (wildcard matches the version so it isn't hardcoded)
COPY target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]