#!/usr/bin/env bash

function line()
{
  echo "----------------------------"
}

line
echo "Update the milvus-proto/api version"
commitID=$(git ls-remote https://github.com/milvus-io/milvus-proto.git refs/heads/master | cut -f 1)
go get github.com/milvus-io/milvus-proto/go-api@$commitID

line
echo "Update the milvus-proto submodule"
git submodule update --remote