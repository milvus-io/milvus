#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname "$0")
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty

if [ ! -d "$THIRD_PARTY_DIR/milvus-proto" ]; then
  mkdir -p $THIRD_PARTY_DIR
  pushd $THIRD_PARTY_DIR
  git clone https://github.com/milvus-io/milvus-proto.git
  cd milvus-proto
  API_VERSION=$(go list -m github.com/milvus-io/milvus-proto/go-api | awk -F' ' '{print $2}')
  COMMIT_ID=$(echo $API_VERSION | awk -F'-' '{print $3}')
  echo "version: $API_VERSION, commitID: $COMMIT_ID"
  if [ -z $COMMIT_ID ]; then
      git checkout -b $API_VERSION $API_VERSION
  else
      git reset --hard $COMMIT_ID
  fi
  popd
fi