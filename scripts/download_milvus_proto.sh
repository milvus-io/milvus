#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname "$0")
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty
API_VERSION=$(go list -m github.com/milvus-io/milvus-proto/go-api/v2 | awk -F' ' '{print $2}')

if [ ! -d "$THIRD_PARTY_DIR/milvus-proto" ]; then
  mkdir -p $THIRD_PARTY_DIR
  pushd $THIRD_PARTY_DIR
  git clone https://github.com/milvus-io/milvus-proto.git
  cd milvus-proto
  # try tagged version first
  COMMIT_ID=$(git ls-remote https://github.com/milvus-io/milvus-proto.git refs/tags/${API_VERSION} | cut -f 1)
  if [[ -z $COMMIT_ID ]]; then 
    # parse commit from pseudo version (eg v0.0.0-20230608062631-c453ef1b870a => c453ef1b870a)  
    COMMIT_ID=$(echo $API_VERSION | awk -F'-' '{print $3}')
  fi
  echo "version: $API_VERSION, commitID: $COMMIT_ID"
  if [ -z $COMMIT_ID ]; then
      git checkout -b $API_VERSION $API_VERSION
  else
      git reset --hard $COMMIT_ID
  fi
  popd
fi
