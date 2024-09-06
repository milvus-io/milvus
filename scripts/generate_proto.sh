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

INSTALL_PATH="$1"

PROGRAM=$(basename "$0")
GOPATH=$(go env GOPATH)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

export PATH=${INSTALL_PATH}:${GOPATH}/bin:$PATH

echo "using protoc-gen-go: $(which protoc-gen-go)"
echo "using protoc-gen-go-grpc: $(which protoc-gen-go-grpc)"

# official go code ship with the crate, so we need to generate it manually.
pushd ${PROTO_DIR}

mkdir -p etcdpb
mkdir -p indexcgopb
mkdir -p cgopb

mkdir -p internalpb
mkdir -p rootcoordpb

mkdir -p segcorepb
mkdir -p clusteringpb
mkdir -p proxypb

mkdir -p indexpb
mkdir -p datapb
mkdir -p querypb
mkdir -p planpb
mkdir -p streamingpb
mkdir -p workerpb

mkdir -p $ROOT_DIR/cmd/tools/migration/legacy/legacypb

protoc_opt="${PROTOC_BIN} --proto_path=${API_PROTO_DIR} --proto_path=."

${protoc_opt} --go_out=paths=source_relative:./etcdpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./etcdpb etcd_meta.proto || { echo 'generate etcd_meta.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./indexcgopb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./indexcgopb index_cgo_msg.proto || { echo 'generate index_cgo_msg failed '; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./cgopb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./cgopb cgo_msg.proto || { echo 'generate cgo_msg failed '; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./rootcoordpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./rootcoordpb root_coord.proto || { echo 'generate root_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./internalpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./internalpb internal.proto || { echo 'generate internal.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./proxypb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./proxypb proxy.proto|| { echo 'generate proxy.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./indexpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./indexpb index_coord.proto|| { echo 'generate index_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./datapb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./datapb data_coord.proto|| { echo 'generate data_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./querypb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./querypb query_coord.proto|| { echo 'generate query_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./planpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./planpb plan.proto|| { echo 'generate plan.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./segcorepb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./segcorepb segcore.proto|| { echo 'generate segcore.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./clusteringpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./clusteringpb clustering.proto|| { echo 'generate clustering.proto failed'; exit 1; }

${protoc_opt} --go_out=paths=source_relative:./workerpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./workerpb worker.proto|| { echo 'generate worker.proto failed'; exit 1; }

${protoc_opt} --proto_path=$ROOT_DIR/pkg/eventlog/ --go_out=paths=source_relative:../../pkg/eventlog/ --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:../../pkg/eventlog/ event_log.proto || { echo 'generate event_log.proto failed'; exit 1; }
${protoc_opt} --proto_path=$ROOT_DIR/cmd/tools/migration/backend --go_out=paths=source_relative:../../cmd/tools/migration/backend/ --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:../../cmd/tools/migration/backend backup_header.proto || { echo 'generate backup_header.proto failed'; exit 1; }

${protoc_opt} --proto_path=$ROOT_DIR/cmd/tools/migration/legacy/ \
  --go_out=paths=source_relative:../../cmd/tools/migration/legacy/legacypb legacy.proto || { echo 'generate legacy.proto failed'; exit 1; }

${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb schema.proto|| { echo 'generate schema.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb common.proto|| { echo 'generate common.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb segcore.proto|| { echo 'generate segcore.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb clustering.proto|| { echo 'generate clustering.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb index_cgo_msg.proto|| { echo 'generate index_cgo_msg.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb cgo_msg.proto|| { echo 'generate cgo_msg.proto failed'; exit 1; }
${protoc_opt} --cpp_out=$CPP_SRC_DIR/src/pb plan.proto|| { echo 'generate plan.proto failed'; exit 1; }

popd


pushd $ROOT_DIR/pkg/streaming/proto

mkdir -p messagespb
mkdir -p streamingpb

# streaming node message protobuf
${protoc_opt} --go_out=paths=source_relative:./messagespb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./messagespb messages.proto || { echo 'generate messagespb.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./streamingpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./streamingpb streaming.proto || { echo 'generate streamingpb.proto failed'; exit 1; }

popd
