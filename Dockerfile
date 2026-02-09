FROM eclipse-temurin:21-jre-alpine

LABEL org.opencontainers.image.authors="devops@metasolutions.se"
LABEL se.metasolutions.service="rowstore"

RUN mkdir -p /srv/rowstore
WORKDIR /srv/rowstore

COPY target/rowstore-*.jar /srv/rowstore/rowstore.jar

EXPOSE 8282

ENTRYPOINT ["java", "-jar", "/srv/rowstore/rowstore.jar"]
CMD ["--rowstore.config.uri=file:///srv/rowstore/rowstore.json"]
