FROM openjdk:8

WORKDIR /bin

COPY sdks/java/extensions/schemaio-expansion-service/build/libs/beam-sdks-java-extensions-schemaio-expansion-service-2.42.0-SNAPSHOT.jar expansion-service-nats.jar

CMD java -jar expansion-service-nats.jar 3333