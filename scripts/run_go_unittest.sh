#!/usr/bin/env bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# ignore Minio,S3 unittes
MILVUS_DIR="${SCRIPTS_DIR}/../internal/"
echo $MILVUS_DIR
go test -cover "${MILVUS_DIR}/kv/..." "${MILVUS_DIR}/msgstream/..." "${MILVUS_DIR}/master/..." "${MILVUS_DIR}/reader/..." "${MILVUS_DIR}/proxy/..." -failfast
