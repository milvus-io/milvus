#!/usr/bin/env bash
set -euo pipefail

SCRIPTS_DIR=$(dirname "$0")
THIRD_PARTY_DIR=$SCRIPTS_DIR/../cmake_build/thirdparty
PROTO_CHECKOUT=$THIRD_PARTY_DIR/milvus-proto
GO_API_MODULE=$(go list -m -f '{{with .Replace}}{{.Path}}|{{.Version}}|{{.Dir}}{{else}}{{.Path}}|{{.Version}}|{{.Dir}}{{end}}' github.com/milvus-io/milvus-proto/go-api/v3)
IFS='|' read -r MODULE_PATH API_VERSION MODULE_DIR <<< "$GO_API_MODULE"

if [[ "$MODULE_PATH" == github.com/*/milvus-proto/go-api/v3 ]]; then
  PROTO_REPO=https://${MODULE_PATH%/go-api/v3}.git
elif [[ -n "$MODULE_DIR" && -d "${MODULE_DIR%/go-api/v3}/proto" ]]; then
  PROTO_REPO=${MODULE_DIR%/go-api/v3}
elif [[ -n "$MODULE_DIR" && -d "${MODULE_DIR%/go-api}/proto" ]]; then
  PROTO_REPO=${MODULE_DIR%/go-api}
else
  echo "unsupported milvus-proto go-api module replacement: $GO_API_MODULE" >&2
  exit 1
fi

# Try tagged version first.
COMMIT_ID=""
if [[ -n "$API_VERSION" ]]; then
  COMMIT_ID=$(git ls-remote "$PROTO_REPO" "refs/tags/${API_VERSION}" | cut -f 1)
fi
if [[ -n "$API_VERSION" && -z "$COMMIT_ID" ]]; then
  # Parse commit from pseudo version (eg v0.0.0-20230608062631-c453ef1b870a => c453ef1b870a).
  COMMIT_ID=$(echo "$API_VERSION" | awk -F'-' '{print $3}')
fi

if [ ! -d "$PROTO_CHECKOUT" ]; then
  mkdir -p "$THIRD_PARTY_DIR"
  git clone "$PROTO_REPO" "$PROTO_CHECKOUT"
fi

if ! git -C "$PROTO_CHECKOUT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "$PROTO_CHECKOUT exists but is not a git checkout" >&2
  exit 1
fi

if [[ -n "$API_VERSION" ]]; then
  git -C "$PROTO_CHECKOUT" fetch --tags "$PROTO_REPO" '+refs/heads/*:refs/remotes/milvus-proto-src/*'
  echo "module: $MODULE_PATH, repo: $PROTO_REPO, version: $API_VERSION, commitID: $COMMIT_ID"
  if [[ -z "$COMMIT_ID" ]]; then
    git -C "$PROTO_CHECKOUT" checkout -B "$API_VERSION" "$API_VERSION"
  else
    git -C "$PROTO_CHECKOUT" reset --hard "$COMMIT_ID"
  fi
else
  git -C "$PROTO_CHECKOUT" fetch "$PROTO_REPO" HEAD
  echo "module: $MODULE_PATH, repo: $PROTO_REPO, local replace"
  git -C "$PROTO_CHECKOUT" reset --hard FETCH_HEAD
fi
