#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

# ignore MinIO,S3 unittes
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

echo " Go unittest finished"
