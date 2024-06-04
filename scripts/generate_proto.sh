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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

PROTO_DIR=$ROOT_DIR/internal/proto/
API_PROTO_DIR=$ROOT_DIR/cmake_build/thirdparty/milvus-proto/proto
CPP_SRC_DIR=$ROOT_DIR/internal/core
PROTOC_BIN=$ROOT_DIR/cmake_build/bin/protoc

PROGRAM=$(basename "$0")

export GOBIN=$ROOT_DIR/bin

if [ ! -f $GOBIN/protoc-gen-go ]; then 
  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0
fi

if [ ! -f $GOBIN/protoc-gen-go-grpc ]; then
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
fi

# official go code ship with the crate, so we need to generate it manually.
pushd ${PROTO_DIR}

mkdir -p $ROOT_DIR/cmd/tools/migration/legacy/legacypb

protoc_opt="${PROTOC_BIN}
  --proto_path=${API_PROTO_DIR}
  --proto_path=.
  --plugin=protoc-gen-go=$GOBIN/protoc-gen-go
  --plugin=protoc-gen-go-grpc=$GOBIN/protoc-gen-go-grpc"

${protoc_opt} \
--go_out=$ROOT_DIR \
--go_opt=module=github.com/milvus-io/milvus \
--go-grpc_out=$ROOT_DIR \
--go-grpc_opt=module=github.com/milvus-io/milvus,require_unimplemented_servers=false \
etcd_meta.proto \
index_cgo_msg.proto \
cgo_msg.proto \
root_coord.proto \
internal.proto \
proxy.proto \
index_coord.proto \
data_coord.proto \
query_coord.proto \
plan.proto \
segcore.proto \
|| { echo 'generate go proto failed'; exit 1; }

${protoc_opt} --proto_path=$ROOT_DIR/cmd/tools/migration/legacy/ \
  --go_out=paths=source_relative:../../cmd/tools/migration/legacy/legacypb legacy.proto || { echo 'generate legacy.proto failed'; exit 1; }

${protoc_opt} --proto_path=$ROOT_DIR/cmd/tools/migration/backend/ \
  --go_out=$ROOT_DIR --go_opt=module=github.com/milvus-io/milvus backend.proto || { echo 'generate backend.proto failed'; exit 1; }

${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb schema.proto|| { echo 'generate schema.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb common.proto|| { echo 'generate common.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb segcore.proto|| { echo 'generate segcore.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb index_cgo_msg.proto|| { echo 'generate index_cgo_msg.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb cgo_msg.proto|| { echo 'generate cgo_msg.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb plan.proto|| { echo 'generate plan.proto failed'; exit 1; }

popd
