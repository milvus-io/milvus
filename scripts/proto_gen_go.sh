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
mkdir -p internalpb
mkdir -p servicepb
mkdir -p masterpb
mkdir -p indexbuilderpb

${protoc} --go_out=plugins=grpc,paths=source_relative:./commonpb common.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./schemapb schema.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./etcdpb etcd_meta.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./internalpb internal_msg.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./servicepb service_msg.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./servicepb service.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./masterpb master.proto
${protoc} --go_out=plugins=grpc,paths=source_relative:./indexbuilderpb index_builder.proto

popd
