#!/usr/bin/env bash

protoc=`which protoc`
grpc_cpp_plugin=`which grpc_cpp_plugin`

SCRIPTS_DIR=$(dirname "$0")

while getopts "p:g:h" arg; do
  case $arg in
  p)
    protoc=$(readlink -f "${OPTARG}")
    ;;
  g)
    grpc_cpp_plugin=$(readlink -f "${OPTARG}")
    ;;
    h) # help
    echo "

parameter:
-p: protoc path default(`which protoc`)
-g: grpc_cpp_plugin path default(`which grpc_cpp_plugin`)
-h: help

usage:
./build.sh  [-h]
                "
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done


ROOT_DIR="$SCRIPTS_DIR/.."
source $SCRIPTS_DIR/common.sh

#protoc=${ROOT_DIR}/proxy/cmake_build/thirdparty/grpc/grpc-build/third_party/protobuf/protoc
#grpc_cpp_plugin=${ROOT_DIR}/proxy/cmake_build/thirdparty/grpc/grpc-build/grpc_cpp_plugin

echo "generate cpp code..."

OUTDIR=${ROOT_DIR}/proxy/src/grpc

cd $ROOT_DIR
GRPC_INCLUDE=.:.
rm -rf proto-cpp && mkdir -p proto-cpp

PB_FILES=()
GRPC_FILES=("message.proto")

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
