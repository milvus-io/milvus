#!/usr/bin/env bash

function line()
{
  echo "----------------------------"
}

line
echo "Update the milvus/api version"
commitID=$(git ls-remote https://github.com/milvus-io/milvus-proto.git refs/heads/$GIT_BRANCH | cut -f 1)
go get github.com/milvus-io/milvus-proto/go-api@$commitID

SCRIPTS_DIR=$(dirname "$0")
EXAMPLE_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty/protobuf/protobuf-src/examples
rm -rf $EXAMPLE_DIR
go mod tidy
go get github.com/quasilyte/go-ruleguard/dsl@v0.3.21