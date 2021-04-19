#!/usr/bin/env bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

MILVUS_CORE_DIR="${SCRIPTS_DIR}/../internal/core"
CORE_INSTALL_PREFIX="${MILVUS_CORE_DIR}/output"
UNITTEST_DIRS=("${CORE_INSTALL_PREFIX}/unittest")

# Currently core will install target lib to "core/output/lib"
if [ -d "${CORE_INSTALL_PREFIX}/lib" ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${CORE_INSTALL_PREFIX}/lib
fi

# run unittest
for UNITTEST_DIR in "${UNITTEST_DIRS[@]}"; do
  if [ ! -d "${UNITTEST_DIR}" ]; then
	echo "The unittest folder does not exist!"
    exit 1
  fi
  for test in `ls ${UNITTEST_DIR}`; do
    echo $test " running..."
    # run unittest
#    ${UNITTEST_DIR}/${test}
    if [ $? -ne 0 ]; then
        echo ${UNITTEST_DIR}/${test} "run failed"
        exit 1
    fi
  done
done
