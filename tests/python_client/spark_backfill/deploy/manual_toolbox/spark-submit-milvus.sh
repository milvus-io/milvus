#!/usr/bin/env bash

set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
CONNECTOR_ROOT="${CONNECTOR_ROOT:-/opt/spark-milvus}"
CONNECTOR_JAR="$CONNECTOR_ROOT/jars/spark-connector-assembly.jar"
NATIVE_DIR="$CONNECTOR_ROOT/native"

test -f "$CONNECTOR_JAR"
test -f "$NATIVE_DIR/libmilvus-storage.so"
test -f "$NATIVE_DIR/libmilvus-storage-jni.so"

export LD_LIBRARY_PATH="$NATIVE_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

connector_jar_args=(--jars "$CONNECTOR_JAR")
for argument in "$@"; do
  if [[ "$argument" == "--class" ]]; then
    # A JVM application such as BackfillApp receives the Connector JAR as its
    # primary application resource. Adding the same JAR through --jars loads
    # duplicate BackfillConfig classes in separate ChildFirst classloaders.
    connector_jar_args=()
    break
  fi
done

exec "$SPARK_HOME/bin/spark-submit" \
  --master 'local[2]' \
  --packages org.apache.hadoop:hadoop-aws:3.4.1 \
  --exclude-packages software.amazon.awssdk:bundle \
  "${connector_jar_args[@]}" \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=$NATIVE_DIR" \
  --conf "spark.driver.extraLibraryPath=$NATIVE_DIR" \
  --conf "spark.executor.extraLibraryPath=$NATIVE_DIR" \
  --conf "spark.executorEnv.LD_LIBRARY_PATH=$NATIVE_DIR" \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=false \
  --conf spark.jars.ivy=/tmp/spark-local/ivy \
  --conf spark.local.dir=/tmp/spark-local \
  "$@"
