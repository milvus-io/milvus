#!/usr/bin/env bash
SCRIPTS_DIR=$(dirname "$0")

while getopts "p:h" arg; do
  case $arg in
  p)
    protoc=$(readlink -f "${OPTARG}")
    ;;
    h) # help
    echo "

parameter:
-p: protoc path default("protoc")
-h: help

usage:
./build.sh -p protoc [-h]
                "
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done


PROTO_DIR=$SCRIPTS_DIR/../internal/proto/

PROGRAM=$(basename "$0")
GOPATH=$(go env GOPATH)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

export PATH=${GOPATH}/bin:$PATH
echo `which protoc-gen-go`


# Although eraftpb.proto is copying from raft-rs, however there is no
# official go code ship with the crate, so we need to generate it manually.
pushd ${PROTO_DIR}

PB_FILES=("message.proto" "master.proto")

ret=0

function gen_pb() {
    base_name=$(basename $1 ".proto")
    mkdir -p ./$base_name
    ${protoc} --go_out=plugins=grpc,paths=source_relative:./$base_name $1 || ret=$?
  }

for file in ${PB_FILES[@]}
    do
    echo $file
    gen_pb $file
done

popd

exit $ret
