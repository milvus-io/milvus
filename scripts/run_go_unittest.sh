#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Exit immediately for non zero status
set -e

BASEDIR=$(dirname "$0")
source $BASEDIR/setenv.sh

if [[ $(uname -s) == "Darwin" ]]; then
    export MallocNanoZone=0
fi

if [[ "$(uname -m)" == "arm64" ]]; then
  APPLE_SILICON_FLAG="-tags dynamic"
fi

# ignore MinIO,S3 unittes
MILVUS_DIR="${ROOT_DIR}/internal/"
echo "Running go unittest under $MILVUS_DIR"

TEST_ALL=1
TEST_TAG="ALL"

while getopts "t:h" arg; do
  case $arg in
  t)
    TEST_ALL=0
    TEST_TAG=$OPTARG
    ;;
  h) # help
    echo "
parameter:
-t: test tag(default: all)
-h: help

usage:
./go_run_unittest.sh -t \${TEST_TAG}
                "
    exit 0
    ;;
  ?)
    echo "ERROR! unknown argument"
    exit 1
    ;;
  esac
done

function test_proxy()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/proxy/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/proxy/..." -failfast
}

function test_querycoordv2()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/querycoordv2/..." -failfast
}

function test_querynode()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/querynode/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/querynode/..." -failfast
}


function test_kv()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/kv/..." -failfast
}

function test_mq()
{
go test -race -cover ${APPLE_SILICON_FLAG} $(go list "${MILVUS_DIR}/mq/..." | grep -v kafka)  -failfast
}

function test_storage()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/storage" -failfast
}

function test_allocator()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/allocator/..." -failfast
}

function test_tso()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/tso/..." -failfast
}

function test_config()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/config/..." -failfast
}

function test_util()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/funcutil/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/paramtable/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/retry/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/sessionutil/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/trace/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/typeutil/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/importutil/..." -failfast
}

function test_datanode
{

go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/datanode/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/datanode/..." -failfast

}

function test_indexnode()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/indexnode/..." -failfast
}

function test_rootcoord()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/rootcoord/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/rootcoord" -failfast
}

function test_datacoord()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/datacoord/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/datacoord/..." -failfast
}

function test_querycoord()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/querycoord/..." -failfast
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/querycoord/..." -failfast
}

function test_indexcoord()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/indexcoord/..." -failfast
}

function test_metastore()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/metastore/..." -failfast
}

function test_all()
{
test_proxy
test_querynode
test_datanode
test_indexnode
test_rootcoord
test_querycoord
test_datacoord
test_indexcoord
test_kv
test_mq
test_storage
test_allocator
test_tso
test_config
test_util
test_metastore
test_querycoordv2
}



case "${TEST_TAG}" in
    proxy)
	test_proxy
	;;
    querynode)
	test_querynode
        ;;
    datanode)
	test_datanode
        ;;
    indexnode)
	test_indexnode
        ;;
    rootcoord)
	test_rootcoord
        ;;
    querycoord)
	test_querycoord
        ;;
    datacoord)
	test_datacoord
        ;;
    indexcoord)
	test_indexcoord
        ;;
    kv)
	test_kv
        ;;
    mq)
	test_mq
        ;;
    storage)
	test_storage
        ;;
    allocator)
	test_allocator
        ;;
    tso)
	test_tso
        ;;
    config)
	test_config
        ;;
    util)
	test_util
        ;;
    metastore)
	test_metastore
        ;;
    *)   echo "Test All";
	test_all
    ;;
esac


echo " Go unittest finished"
