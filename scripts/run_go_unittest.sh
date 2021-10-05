#!/usr/bin/env bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

# ignore Minio,S3 unittes
MILVUS_DIR="${ROOT_DIR}/internal/"
echo "Running go unittest under $MILVUS_DIR"

go test -race -cover "${MILVUS_DIR}/allocator/..." -failfast
go test -race -cover "${MILVUS_DIR}/kv/..." -failfast
go test -race -cover "${MILVUS_DIR}/msgstream/..." -failfast
go test -race -cover "${MILVUS_DIR}/storage" -failfast
go test -race -cover "${MILVUS_DIR}/tso/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/dablooms/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/funcutil/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/mqclient/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/paramtable/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/retry/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/rocksmq/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/sessionutil/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/trace/..." -failfast
go test -race -cover "${MILVUS_DIR}/util/typeutil/..." -failfast

# TODO: remove to distributed
#go test -race -cover "${MILVUS_DIR}/proxy/..." -failfast
go test -race -cover "${MILVUS_DIR}/datanode/..." -failfast
go test -race -cover "${MILVUS_DIR}/indexnode/..." -failfast
go test -race -cover "${MILVUS_DIR}/querynode/..." -failfast

go test -race -cover -v "${MILVUS_DIR}/distributed/rootcoord" -failfast
go test -race -cover -v "${MILVUS_DIR}/rootcoord" -failfast
go test -race -cover -v "${MILVUS_DIR}/datacoord/..." -failfast
go test -race -cover -v "${MILVUS_DIR}/indexcoord/..." -failfast
