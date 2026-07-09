FROM eclipse-temurin:17-jre
WORKDIR /app
# 把打好的 jar 复制进镜像(星号匹配版本号,省得写死)
COPY target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]