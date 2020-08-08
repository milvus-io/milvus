#!/bin/bash

set -ex

export CCACHE_COMPRESS=1
export CCACHE_COMPRESSLEVEL=5
export CCACHE_COMPILERCHECK=content
export CCACHE_SLOPPINESS=file_stat_matches,file_stat_matches_ctime,time_macros,include_file_mtime,include_file_ctime
export CCACHE_BASEDIR=${WORKSPACE:=""}
export PATH=/usr/lib/ccache/:$PATH

set +ex
