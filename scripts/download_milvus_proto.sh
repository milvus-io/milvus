#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname "$0")
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty
API_VERSION=$(go list -m github.com/milvus-io/milvus-proto/go-api/v3 | awk -F' ' '{print $2}')
PROTO_REPO=https://github.com/milvus-io/milvus-proto.git

# Try tagged version first.
COMMIT_ID=$(git ls-remote "$PROTO_REPO" refs/tags/${API_VERSION} | cut -f 1)
if [[ -z $COMMIT_ID ]]; then
  # Parse commit from pseudo version (eg v0.0.0-20230608062631-c453ef1b870a => c453ef1b870a).
  COMMIT_ID=$(echo $API_VERSION | awk -F'-' '{print $3}')
fi

if [ ! -d "$THIRD_PARTY_DIR/milvus-proto" ]; then
  mkdir -p $THIRD_PARTY_DIR
  pushd $THIRD_PARTY_DIR
  git clone "$PROTO_REPO"
  popd
fi

pushd "$THIRD_PARTY_DIR/milvus-proto"
git fetch --tags origin
echo "version: $API_VERSION, commitID: $COMMIT_ID"
if [ -z $COMMIT_ID ]; then
    git checkout -B "$API_VERSION" "$API_VERSION"
else
    git reset --hard "$COMMIT_ID"
fi
popd
