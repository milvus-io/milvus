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

# ignore MinIO,S3 unittests
MILVUS_DIR="${ROOT_DIR}/internal/"
PKG_DIR="${ROOT_DIR}/pkg/"
echo "Running go unittest under $MILVUS_DIR & $PKG_DIR"

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
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/proxy/..." -failfast -count=1 
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/proxy/..." -failfast -count=1  
}

function test_querynode()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/querynodev2/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/querynode/..." -failfast -count=1
}


function test_kv()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/kv/..." -failfast -count=1
}

function test_mq()
{
go test -race -cover ${APPLE_SILICON_FLAG} $(go list "${MILVUS_DIR}/mq/..." | grep -v kafka)  -failfast -count=1
}

function test_storage()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/storage" -failfast -count=1
}

function test_allocator()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/allocator/..." -failfast -count=1
}

function test_tso()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/tso/..." -failfast -count=1
}

function test_config()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/config/..." -failfast -count=1
}

function test_util()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/funcutil/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/paramtable/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${PKG_DIR}/util/retry/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/sessionutil/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/typeutil/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/util/importutil/..." -failfast -count=1
}

function test_pkg()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${PKG_DIR}/common/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${PKG_DIR}/config/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${PKG_DIR}/log/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${PKG_DIR}/mq/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${PKG_DIR}/tracer/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${PKG_DIR}/util/..." -failfast -count=1
}

function test_datanode
{

go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/datanode/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/datanode/..." -failfast -count=1

}

function test_indexnode()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/indexnode/..." -failfast -count=1
}

function test_rootcoord()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/rootcoord/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/rootcoord" -failfast
}

function test_datacoord()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/datacoord/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/datacoord/..." -failfast -count=1
}

function test_querycoord()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/distributed/querycoord/..." -failfast -count=1
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/querycoordv2/..." -failfast -count=1
}

#function test_indexcoord()
#{
#go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/indexcoord/..." -failfast
#}

function test_metastore()
{
go test -race -cover ${APPLE_SILICON_FLAG} "${MILVUS_DIR}/metastore/..." -failfast -count=1
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
#test_indexcoord
test_kv
test_mq
test_storage
test_allocator
test_tso
test_config
test_util
test_pkg
test_metastore
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
#    indexcoord)
#	test_indexcoord
#        ;;
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
    pkg)
    test_pkg
        ;;
    metastore)
	test_metastore
        ;;
    *)   echo "Test All";
	test_all
    ;;
esac


echo " Go unittest finished"
