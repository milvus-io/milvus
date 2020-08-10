#!/bin/bash

set -ex

export CCACHE_COMPRESS=1
export CCACHE_COMPRESSLEVEL=5
export CCACHE_COMPILERCHECK=content
export PATH=/usr/lib/ccache/:$PATH
export CCACHE_BASEDIR=${WORKSPACE:=""}

set +ex
