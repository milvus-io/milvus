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
CWRAPPER_UNITTEST="${SCRIPTS_DIR}/../internal/storage/cwrapper/output/wrapper_test"

# currently core will install target lib to "core/output/lib"
if [ -d "${CORE_INSTALL_PREFIX}/lib" ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${CORE_INSTALL_PREFIX}/lib
fi

# run unittest
for UNITTEST_DIR in "${UNITTEST_DIRS[@]}"; do
  if [ ! -d "${UNITTEST_DIR}" ]; then
	echo "The unittest folder does not exist!"
    exit 1
  fi

  echo "Running all unittest ..."
  ${UNITTEST_DIR}/all_tests
  if [ $? -ne 0 ]; then
      echo ${UNITTEST_DIR}/all_tests "run failed"
      exit 1
  fi
done

# run cwrapper unittest
if [ -f ${CWRAPPER_UNITTEST} ];then
  echo "Running cwrapper unittest: ${CWRAPPER_UNITTEST}"
  ${CWRAPPER_UNITTEST}
  if [ $? -ne 0 ]; then
      echo ${CWRAPPER_UNITTEST} " run failed"
      exit 1
  fi
fi
