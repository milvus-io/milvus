#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname "$0")
ROOT_DIR=$SCRIPTS_DIR/..
source $SCRIPTS_DIR/common.sh

#protoc=protoc
protoc=${ROOT_DIR}/proxy/cmake_build/thirdparty/grpc/grpc-build/third_party/protobuf/protoc

push $SCRIPTS_DIR/..
KVPROTO_ROOT=`pwd`
pop

PROGRAM=$(basename "$0")
GOPATH=$(go env GOPATH)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

GO_PREFIX_PATH=github.com/czs007/suvlim/pkg
#export PATH=$KVPROTO_ROOT/_tools/bin:$GOPATH/bin:$PATH

function collect() {
    file=$(basename $1)
    base_name=$(basename $file ".proto")
    mkdir -p ../pkg/$base_name
    if [ -z $GO_OUT_M ]; then
        GO_OUT_M="M$file=$GO_PREFIX_PATH/$base_name"
    else
        GO_OUT_M="$GO_OUT_M,M$file=$GO_PREFIX_PATH/$base_name"
    fi
}

# Although eraftpb.proto is copying from raft-rs, however there is no
# official go code ship with the crate, so we need to generate it manually.
cd ../proto

PB_FILES=("message.proto")
GRPC_FILES=("pdpb.proto" "metapb.proto")
ALL_FILES=("${PB_FILES[@]}")
ALL_FILES+=("${GRPC_FILES[@]}")

for file in ${ALL_FILES[@]}
    do
    collect $file
done

ret=0

function replace(){
    cd ../pkg/$base_name
    sed_inplace -E 's/import fmt \"fmt\"//g' *.pb*.go
    sed_inplace -E 's/import io \"io\"//g' *.pb*.go
    sed_inplace -E 's/import math \"math\"//g' *.pb*.go
    #goimports -w *.pb*.go
    cd ../../proto
}

function gen_pb() {
    base_name=$(basename $1 ".proto")
    $protoc -I.:.   --go_out=plugins=$GO_OUT_M:../pkg/$base_name $1 || ret=$?
    #replace
  }

function gen_grpc() {
    base_name=$(basename $1 ".proto")
    $protoc -I.:.   --go_out=plugins=grpc,$GO_OUT_M:../pkg/${base_name} $1 || ret=$?
    #replace
  }

for file in ${PB_FILES[@]}
    do
    echo $file
    gen_pb $file
done

for file in ${GRPC_FILES[@]}
    do
    echo $file
    gen_grpc $file
done


exit $ret
