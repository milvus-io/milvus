#!/bin/bash

set -ex

if [[ "${TRAVIS_OS_NAME}" == "linux" ]]; then
  export CCACHE_COMPRESS=1
  export CCACHE_COMPRESSLEVEL=5
  export CCACHE_COMPILERCHECK=content
  ccache --show-stats
fi

set +ex
