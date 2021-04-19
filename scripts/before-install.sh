#!/bin/bash

set -ex

export CCACHE_COMPRESS=${CCACHE_COMPRESS:="1"}
export CCACHE_COMPRESSLEVEL=${CCACHE_COMPRESSLEVEL:="5"}
export CCACHE_COMPILERCHECK=${CCACHE_COMPILERCHECK:="content"}
export CCACHE_MAXSIZE=${CCACHE_MAXSIZE:="2G"}
export CCACHE_DIR=${CCACHE_DIR:="${HOME}/.ccache"}
export http_proxy="http://proxy.zilliz.tech:1088"
export https_proxy="http://proxy.zilliz.tech:1088"

set +ex
