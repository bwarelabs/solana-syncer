#!/bin/bash

exec java -ea -XX:+UseZGC -XX:+ZGenerational $JVM_ARGS \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -jar target/syncer-1.0-SNAPSHOT.jar "$@"
