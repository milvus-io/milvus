#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname "$0")
ROOT_DIR="$SCRIPTS_DIR/.."
source $SCRIPTS_DIR/common.sh

#protoc=protoc
protoc=${ROOT_DIR}/proxy/cmake_build/thirdparty/grpc/grpc-build/third_party/protobuf/protoc
grpc_cpp_plugin=${ROOT_DIR}/proxy/cmake_build/thirdparty/grpc/grpc-build/grpc_cpp_plugin

echo "generate cpp code..."

OUTDIR=${ROOT_DIR}/proxy/src/grpc

GRPC_INCLUDE=.:.
#GRPC_INCLUDE=.:../include

cd $ROOT_DIR
rm -rf proto-cpp && mkdir -p proto-cpp

PB_FILES=()
GRPC_FILES=("hello.proto" "master.proto" "message.proto")

ALL_FILES=("${PB_FILES[@]}")
ALL_FILES+=("${GRPC_FILES[@]}")

for file in ${ALL_FILES[@]}
    do
    cp proto/$file proto-cpp/
done

push proto-cpp

#mkdir -p ../pkg/cpp

for file in ${PB_FILES[@]}
    do
    echo $file
    $protoc -I${GRPC_INCLUDE} --cpp_out $OUTDIR *.proto || exit $?
done

for file in ${GRPC_FILES[@]}
    do
    echo $file
    $protoc -I${GRPC_INCLUDE} --cpp_out $OUTDIR *.proto || exit $?
    $protoc -I${GRPC_INCLUDE} --grpc_out $OUTDIR --plugin=protoc-gen-grpc=${grpc_cpp_plugin} *.proto || exit $?
done


pop

rm -rf proto-cpp
