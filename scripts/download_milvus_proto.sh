#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname "$0")
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty/

mkdir -p $THIRD_PARTY_DIR

pushd $THIRD_PARTY_DIR

rm -rf milvus-proto

git clone -b $GIT_BRANCH https://github.com/milvus-io/milvus-proto.git

popd