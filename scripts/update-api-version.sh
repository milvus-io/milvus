#!/usr/bin/env bash

function line()
{
  echo "----------------------------"
}

version=$@
if [[ $version == "" ]]; then 
    echo "milvus/proto/go-api version not provided"
    line 
    echo "example:"
    echo "       make update-milvus-api PROTO_API_VERSION=v2.3.0-dev.1"
    echo "test with untagged commit:" 
    echo "       make update-milvus-api PROTO_API_VERSION=\${commitID}"
    line
    exit 1
fi

commitID=$(git ls-remote https://github.com/milvus-io/milvus-proto.git refs/tags/${version} | cut -f 1) 

line
echo "Update the milvus-proto/go-api/v2@${version}"
if [[ $commitID == "" ]]; then
    echo "${version} is not a valid tag, try to use it as commit ID"
    commitID=$version
    go get -u github.com/milvus-io/milvus-proto/go-api/v2@$commitID
else
    go get -u github.com/milvus-io/milvus-proto/go-api/v2@$version
fi

SCRIPTS_DIR=$(dirname "$0")
EXAMPLE_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty/protobuf/protobuf-src/examples
rm -rf $EXAMPLE_DIR
go mod tidy

line echo "Update the milvus-proto repo"
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty

pushd $THIRD_PARTY_DIR/milvus-proto
    git fetch
    git checkout -b $version $commitID
popd
