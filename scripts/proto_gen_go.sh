#!/usr/bin/env bash
SCRIPTS_DIR=$(dirname "$0")

PROTO_DIR=$SCRIPTS_DIR/../internal/proto/

PROGRAM=$(basename "$0")
GOPATH=$(go env GOPATH)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

export PATH=${GOPATH}/bin:$PATH
echo `which protoc-gen-go`

# official go code ship with the crate, so we need to generate it manually.
pushd ${PROTO_DIR}

mkdir -p commonpb
mkdir -p schemapb
mkdir -p etcdpb
mkdir -p indexcgopb

mkdir -p internalpb
mkdir -p servicepb
mkdir -p masterpb
mkdir -p indexbuilderpb
mkdir -p writerpb


mkdir -p internalpb2
mkdir -p milvuspb
mkdir -p proxypb
mkdir -p masterpb2

mkdir -p indexpb
mkdir -p datapb
mkdir -p querypb


${protoc} --go_out=plugins=grpc,paths=source_relative:./commonpb common.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./schemapb schema.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./etcdpb etcd_meta.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./indexcgopb index_cgo_msg.proto

${protoc} --go_out=plugins=grpc,paths=source_relative:./internalpb internal_msg.proto

PROTOBUF_GOOGLE_SRC=${SCRIPTS_DIR}/../cmake_build/thirdparty/protobuf/protobuf-src/src/google
if [ -d ${PROTOBUF_GOOGLE_SRC} ]; then
  echo ${PROTOBUF_GOOGLE_SRC}
  ln -snf ${PROTOBUF_GOOGLE_SRC} google
  ${protoc} --go_out=plugins=grpc,paths=source_relative:./internalpb msg_header.proto
  unlink google
fi

${protoc} --go_out=plugins=grpc,paths=source_relative:./servicepb service_msg.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./servicepb service.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./masterpb master.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./indexbuilderpb index_builder.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./writerpb write_node.proto

${protoc} --go_out=plugins=grpc,paths=source_relative:./internalpb2 internal.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./milvuspb milvus.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./proxypb proxy_service.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./masterpb2 master_service.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./indexpb index_service.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./datapb data_service.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./querypb query_service.proto

popd
