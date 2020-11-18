#!/usr/bin/env bash

SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

GO_SRC_DIR="${SCRIPTS_DIR}/$1"
if test -z "$(gofmt -d $GO_SRC_DIR)"; then
  exit 0
else
  gofmt -d $GO_SRC_DIR
  echo "Please format your code by gofmt!"
  exit 1
fi
