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
    update_version=$commitID
else
    update_version=$version
fi

SCRIPTS_DIR=$(dirname "$0")
PROJECT_ROOT=$(cd "$SCRIPTS_DIR/.." && pwd)

# Update all 4 go.mod files
echo "Updating milvus-proto version in all go.mod files..."

# 1. Update main go.mod
echo "Updating main go.mod..."
cd "$PROJECT_ROOT"
go get -u github.com/milvus-io/milvus-proto/go-api/v2@$update_version
go mod tidy

# 2. Update client/go.mod
echo "Updating client/go.mod..."
cd "$PROJECT_ROOT/client"
go get -u github.com/milvus-io/milvus-proto/go-api/v2@$update_version
go mod tidy

# 3. Update pkg/go.mod
echo "Updating pkg/go.mod..."
cd "$PROJECT_ROOT/pkg"
go get -u github.com/milvus-io/milvus-proto/go-api/v2@$update_version
go mod tidy

# 4. Update tests/go_client/go.mod
echo "Updating tests/go_client/go.mod..."
cd "$PROJECT_ROOT/tests/go_client"
go get -u github.com/milvus-io/milvus-proto/go-api/v2@$update_version
go mod tidy

# Return to project root
cd "$PROJECT_ROOT"

# Clean up protobuf examples directory
EXAMPLE_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty/protobuf/protobuf-src/examples
rm -rf $EXAMPLE_DIR

line 
echo "Update the milvus-proto repo"
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty

if [ -d "$THIRD_PARTY_DIR/milvus-proto" ]; then
    pushd $THIRD_PARTY_DIR/milvus-proto
        git fetch
        git checkout -b $version $commitID 2>/dev/null || git checkout $commitID
    popd
else
    echo "Warning: milvus-proto directory not found at $THIRD_PARTY_DIR/milvus-proto"
fi

line
echo "Successfully updated milvus-proto version to $update_version in all go.mod files:"
echo "  - go.mod"
echo "  - client/go.mod" 
echo "  - pkg/go.mod"
echo "  - tests/go_client/go.mod"
