#!/bin/bash

set -ex

export CCACHE_COMPRESS=${CCACHE_COMPRESS:="1"}
export CCACHE_COMPRESSLEVEL=${CCACHE_COMPRESSLEVEL:="5"}
export CCACHE_COMPILERCHECK=${CCACHE_COMPILERCHECK:="content"}
export CCACHE_MAXSIZE=${CCACHE_MAXSIZE:="2G"}
export CCACHE_DIR=${CCACHE_DIR:="${HOME}/.ccache"}
export GOPROXY="https://goproxy.cn"

set +ex
