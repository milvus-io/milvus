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

PROTO_DIR=$ROOT_DIR/pkg/proto
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

GENERATED_BACKUP_DIR=""
GENERATED_FILES=()

cleanup_generated_backup() {
    if [[ -n "${GENERATED_BACKUP_DIR}" && -d "${GENERATED_BACKUP_DIR}" ]]; then
        rm -rf "${GENERATED_BACKUP_DIR}"
    fi
}

backup_generated_files() {
    GENERATED_BACKUP_DIR=$(mktemp -d "${ROOT_DIR}/cmake_build/.proto-backup.XXXXXX")

    while IFS= read -r -d '' file; do
        GENERATED_FILES+=("$file")
        rel_path="${file#"${ROOT_DIR}/"}"
        mkdir -p "${GENERATED_BACKUP_DIR}/$(dirname "${rel_path}")"
        cp -p "$file" "${GENERATED_BACKUP_DIR}/${rel_path}"
    done < <(find \
        "${ROOT_DIR}/pkg/proto" \
        "${ROOT_DIR}/pkg/eventlog" \
        "${ROOT_DIR}/cmd/tools/migration/backend" \
        "${ROOT_DIR}/cmd/tools/migration/legacy/legacypb" \
        "${CPP_SRC_DIR}/src/pb" \
        -type f \( -name "*.pb.go" -o -name "*_grpc.pb.go" -o -name "*.pb.cc" -o -name "*.pb.h" \) -print0)
}

restore_unchanged_generated_files() {
    local file rel_path backup_path

    for file in "${GENERATED_FILES[@]}"; do
        rel_path="${file#"${ROOT_DIR}/"}"
        backup_path="${GENERATED_BACKUP_DIR}/${rel_path}"
        if [[ -f "${backup_path}" && -f "$file" ]] && cmp -s "${backup_path}" "$file"; then
            cp -p "${backup_path}" "$file"
        fi
    done
}

trap cleanup_generated_backup EXIT

# official go code ship with the crate, so we need to generate it manually.
pushd ${PROTO_DIR}

mkdir -p ./etcdpb
mkdir -p ./indexcgopb
mkdir -p ./cgopb
mkdir -p ./internalpb
mkdir -p ./rootcoordpb
mkdir -p ./modelservicepb
mkdir -p ./segcorepb
mkdir -p ./clusteringpb
mkdir -p ./proxypb
mkdir -p ./indexpb
mkdir -p ./datapb
mkdir -p ./querypb
mkdir -p ./planpb
mkdir -p ./workerpb
mkdir -p ./messagespb
mkdir -p ./streamingpb
mkdir -p ./viewpb
mkdir -p $ROOT_DIR/cmd/tools/migration/legacy/legacypb

protoc_opt="${PROTOC_BIN} --proto_path=${API_PROTO_DIR} --proto_path=."

backup_generated_files

${protoc_opt} --go_out=paths=source_relative:./etcdpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./etcdpb etcd_meta.proto || { echo 'generate etcd_meta.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./indexcgopb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./indexcgopb index_cgo_msg.proto || { echo 'generate index_cgo_msg failed '; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./cgopb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./cgopb cgo_msg.proto || { echo 'generate cgo_msg failed '; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./rootcoordpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./rootcoordpb root_coord.proto || { echo 'generate root_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./modelservicepb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./modelservicepb model_service.proto || { echo 'generate model_service.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./internalpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./internalpb internal.proto || { echo 'generate internal.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./proxypb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./proxypb proxy.proto|| { echo 'generate proxy.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./indexpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./indexpb index_coord.proto|| { echo 'generate index_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./datapb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./datapb data_coord.proto|| { echo 'generate data_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./querypb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./querypb query_coord.proto|| { echo 'generate query_coord.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./planpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./planpb plan.proto|| { echo 'generate plan.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./segcorepb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./segcorepb segcore.proto|| { echo 'generate segcore.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./clusteringpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./clusteringpb clustering.proto|| { echo 'generate clustering.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./messagespb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./messagespb messages.proto || { echo 'generate messages.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./streamingpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./streamingpb streaming.proto || { echo 'generate streamingpb.proto failed'; exit 1; }
${protoc_opt} --go_out=paths=source_relative:./viewpb --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./viewpb view.proto || { echo 'generate viewpb.proto failed'; exit 1; }
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

restore_unchanged_generated_files

popd
