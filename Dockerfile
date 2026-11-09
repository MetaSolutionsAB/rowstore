FROM eclipse-temurin:25-jre-alpine

LABEL org.opencontainers.image.authors="devops@metasolutions.se"
LABEL se.metasolutions.service="rowstore"

# RowStore runs as an unprivileged user with a fixed UID/GID, so that mounted files can be made readable for it
RUN addgroup -S -g 10001 rowstore && adduser -S -u 10001 -G rowstore -h /srv/rowstore rowstore

WORKDIR /srv/rowstore

# The jar stays owned by root (readable, but not writable for the service user)
COPY target/rowstore-*.jar /srv/rowstore/rowstore.jar

USER rowstore

# The configuration is read from here unless --rowstore.config.uri=<URI> is passed or the variable is overridden; 1.x
# container definitions set ROWSTORE_CONFIG_URI (1.x required it or -c), e.g. to file:///srv/rowstore/etc/rowstore.json
ENV ROWSTORE_CONFIG_URI=file:///srv/rowstore/rowstore.json

# JVM options, e.g. -Xmx2g or -Duser.timezone=Europe/Stockholm; options in JAVA_OPTS take precedence over the
# default heap of 75% of the container memory
ENV JAVA_OPTS=""

# Port 8080 as in the 1.x images (the JAR's own default is 8282); --server.port=<port> passed to "docker run" takes
# precedence over this environment variable
ENV SERVER_PORT=8080

EXPOSE 8080

# The base image's entrypoint imports the certificates in /certificates/*.crt into a truststore of the JVM if
# USE_SYSTEM_CA_CERTS is set (as non-root, into a copy in /tmp), then runs java. Arguments of "docker run" are passed
# to RowStore (e.g. --server.port=9000). JAVA_OPTS is split into words but not glob-expanded (set -f). Both shells are
# replaced by java (exec), so that java receives the stop signal; give it at least 70 seconds to stop
# (docker stop -t 70, stop_grace_period: 70s)
ENTRYPOINT ["/__cacert_entrypoint.sh", "sh", "-c", "set -f; exec java -XX:MaxRAMPercentage=75 $JAVA_OPTS -jar /srv/rowstore/rowstore.jar \"$@\"", "rowstore"]
