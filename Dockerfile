FROM maven:3.8.4-openjdk-17 as builder
LABEL maintainer="contact@bittich.be"

WORKDIR /app

COPY pom.xml .

RUN mvn -B dependency:resolve-plugins dependency:resolve

COPY ./src ./src

RUN mvn package -DskipTests

FROM ibm-semeru-runtimes:open-17-jre

RUN mkdir /opt/shareclasses

WORKDIR /app

COPY --from=builder /app/target/app.jar ./app.jar

ENTRYPOINT [ "java", "-Xshareclasses:cacheDir=/opt/shareclasses", "-jar","/app/app.jar"]
