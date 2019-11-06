#!/usr/bin/env bash

set -ex

source $TRAVIS_BUILD_DIR/ci/travis/travis_env_common.sh

only_library_mode=no

while true; do
    case "$1" in
	--only-library)
	    only_library_mode=yes
	    shift ;;
	*) break ;;
    esac
done

BUILD_COMMON_FLAGS="-t ${MILVUS_BUILD_TYPE} -o ${MILVUS_INSTALL_PREFIX} -b ${MILVUS_BUILD_DIR}"

if [ $only_library_mode == "yes" ]; then
  ${TRAVIS_BUILD_DIR}/ci/scripts/build.sh ${BUILD_COMMON_FLAGS}
else
  ${TRAVIS_BUILD_DIR}/ci/scripts/build.sh ${BUILD_COMMON_FLAGS} -u -c
fi