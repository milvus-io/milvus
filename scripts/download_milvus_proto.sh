#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname "$0")
PROTO_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty/milvus-proto
API_INFO=$(go list -m -f '{{if and .Replace .Replace.Version}}{{.Replace.Path}} {{.Replace.Version}}{{else}}{{.Path}} {{.Version}}{{end}}' github.com/milvus-io/milvus-proto/go-api/v3) || exit
read -r API_MODULE API_VERSION <<< "$API_INFO"
PROTO_REPO="https://${API_MODULE%/go-api/v3}.git"

# Try tagged version first.
COMMIT_ID=$(git ls-remote "$PROTO_REPO" "refs/tags/$API_VERSION" | cut -f 1)
if [[ -z $COMMIT_ID && $API_VERSION =~ [-.][0-9]{14}-([0-9a-f]{12})$ ]]; then
    COMMIT_ID=${BASH_REMATCH[1]}
fi

if [ ! -d "$PROTO_DIR" ]; then
    mkdir -p "$(dirname "$PROTO_DIR")" || exit
    git clone "$PROTO_REPO" "$PROTO_DIR" || exit
fi

# A previous build may have used a different protocol repository.
git -C "$PROTO_DIR" remote set-url origin "$PROTO_REPO" || exit
git -C "$PROTO_DIR" fetch --tags origin || exit
echo "version: $API_VERSION, commitID: $COMMIT_ID"
if [ -z $COMMIT_ID ]; then
    git -C "$PROTO_DIR" checkout -B "$API_VERSION" "$API_VERSION"
else
    git -C "$PROTO_DIR" reset --hard "$COMMIT_ID"
fi
