FROM eclipse-temurin:25-jre-alpine

LABEL org.opencontainers.image.authors="devops@metasolutions.se"
LABEL se.metasolutions.service="rowstore"

# RowStore runs as an unprivileged user with a fixed UID/GID, so that mounted files can be made readable for it
RUN addgroup -S -g 10001 rowstore && adduser -S -u 10001 -G rowstore -h /srv/rowstore rowstore

WORKDIR /srv/rowstore

COPY target/rowstore-*.jar /srv/rowstore/rowstore.jar

RUN chown -R rowstore:rowstore /srv/rowstore

USER rowstore

# The configuration is read from here unless --rowstore.config.uri=<URI> is passed
ENV ROWSTORE_CONFIG_URI=file:///srv/rowstore/rowstore.json

# JVM options, e.g. -Xmx2g or -Duser.timezone=Europe/Stockholm; options in JAVA_OPTS take precedence over the
# default heap of 75% of the container memory
ENV JAVA_OPTS=""

EXPOSE 8282

# Arguments of "docker run" are passed to RowStore (e.g. --server.port=9000). The shell is replaced by java, so that
# java receives the stop signal; give it at least 70 seconds to stop (docker stop -t 70, stop_grace_period: 70s)
ENTRYPOINT ["sh", "-c", "exec java -XX:MaxRAMPercentage=75 $JAVA_OPTS -jar /srv/rowstore/rowstore.jar \"$@\"", "rowstore"]
