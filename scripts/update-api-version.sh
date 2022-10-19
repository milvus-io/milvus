#!/usr/bin/env bash

function line()
{
  echo "----------------------------"
}

line
echo "Update the milvus-proto/api version"
commitID=$(git ls-remote https://github.com/milvus-io/milvus-proto.git refs/heads/master | cut -f 1)
go get github.com/milvus-io/milvus-proto/go-api@$commitID

SCRIPTS_DIR=$(dirname "$0")
EXAMPLE_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty/protobuf/protobuf-src/examples
rm -rf $EXAMPLE_DIR
go mod tidy

line echo "Update the milvus-proto repo"
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty

pushd $THIRD_PARTY_DIR/milvus-proto
  git checkout master
  git pull origin master
popd