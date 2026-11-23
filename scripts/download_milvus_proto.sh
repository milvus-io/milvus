#!/usr/bin/env bash

set -euo pipefail

SCRIPTS_DIR=$(dirname "$0")
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty
# Companion protocol PRs may pin a fork until their upstream merge. Use the
# same source for C++ generation and Go bindings, including module replacements.
API_MODULE=$(go list -m -f '{{if .Replace}}{{.Replace.Path}}{{else}}{{.Path}}{{end}}' github.com/milvus-io/milvus-proto/go-api/v3)
API_VERSION=$(go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' github.com/milvus-io/milvus-proto/go-api/v3)
if [[ "$API_MODULE" != github.com/*/milvus-proto/go-api/v3 || -z "$API_VERSION" ]]; then
    echo "Expected a versioned milvus-proto module, got $API_MODULE $API_VERSION" >&2
    exit 1
fi
PROTO_REPO="https://${API_MODULE%/go-api/v3}.git"

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
git fetch --tags "$PROTO_REPO" '+refs/heads/*:refs/remotes/proto-source/*'
echo "version: $API_VERSION, commitID: $COMMIT_ID"
if [ -z $COMMIT_ID ]; then
    git checkout -B "$API_VERSION" "$API_VERSION"
else
    git reset --hard "$COMMIT_ID"
fi
popd
