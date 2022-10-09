#!/usr/bin/env bash

function line()
{
  echo "----------------------------"
}

line
echo "Get the latest commitID"
commitID=`git rev-parse --short HEAD`
echo $commitID

line
echo "Update the milvus/api version"
go mod edit -dropreplace=github.com/milvus-io/milvus/api
go get github.com/milvus-io/milvus/api@$commitID