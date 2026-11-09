#!/bin/sh
# Entrypoint of the Docker image (DEPL-2.04), started by the base image's /__cacert_entrypoint.sh (DEPL-2.05).
#
# Configuration: an explicitly set ROWSTORE_CONFIG_URI (or --rowstore.config.uri, which takes precedence over it) is
# used as it is. Otherwise /srv/rowstore/rowstore.json is read, or /srv/rowstore/etc/rowstore.json if only that exists
# (the 1.x images found it there on the classpath). If neither exists, the error names /srv/rowstore/rowstore.json.

if [ -z "$ROWSTORE_CONFIG_URI" ]; then
  if [ ! -e /srv/rowstore/rowstore.json ] && [ -e /srv/rowstore/etc/rowstore.json ]; then
    ROWSTORE_CONFIG_URI=file:///srv/rowstore/etc/rowstore.json
  else
    ROWSTORE_CONFIG_URI=file:///srv/rowstore/rowstore.json
  fi
  export ROWSTORE_CONFIG_URI
fi

# Arguments of "docker run" are passed to RowStore. java replaces the shell, so that it receives the stop signal.
# JAVA_OPTS is split into words on purpose (several options); options in it take precedence over the default heap.
# shellcheck disable=SC2086
exec java -XX:MaxRAMPercentage=75 $JAVA_OPTS -jar /srv/rowstore/rowstore.jar "$@"
