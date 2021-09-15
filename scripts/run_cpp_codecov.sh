#!/usr/bin/env bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

MILVUS_CORE_DIR="${ROOT_DIR}/internal/core"
CORE_INSTALL_PREFIX="${MILVUS_CORE_DIR}/output"
UNITTEST_DIRS=("${CORE_INSTALL_PREFIX}/unittest")
CWRAPPER_UNITTEST="${ROOT_DIR}/internal/storage/cwrapper/output/wrapper_test"

echo "ROOT_DIR = ${ROOT_DIR}"
echo "MILVUS_CORE_DIR = ${MILVUS_CORE_DIR}"
echo "CORE_INSTALL_PREFIX = ${CORE_INSTALL_PREFIX}"
echo "UNITTEST_DIRS = ${UNITTEST_DIRS}"
echo "CWRAPPER_UNITTEST = ${CWRAPPER_UNITTEST}"

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${CORE_INSTALL_PREFIX}/lib
echo "LD_LIBRARY_PATH = ${LD_LIBRARY_PATH}"

LCOV_CMD="lcov"
LCOV_GEN_CMD="genhtml"

FILE_INFO_BASE="base.info"
FILE_INFO_MILVUS="server.info"
FILE_INFO_OUTPUT="output.info"
FILE_INFO_OUTPUT_NEW="output_new.info"
DIR_LCOV_OUTPUT="${ROOT_DIR}/lcov_out"

DIR_GCNO="${ROOT_DIR}/cmake_build"
DIR_UNITTEST="${CORE_INSTALL_PREFIX}/unittest"

# delete old code coverage info files
rm -f FILE_INFO_BASE
rm -f FILE_INFO_MILVUS
rm -f FILE_INFO_OUTPUT
rm -f FILE_INFO_OUTPUT_NEW
rm -rf lcov_out
rm -f FILE_INFO_BASE FILE_INFO_MILVUS FILE_INFO_OUTPUT FILE_INFO_OUTPUT_NEW


# get baseline
${LCOV_CMD} -c -i -d ${DIR_GCNO} -o "${FILE_INFO_BASE}"
if [ $? -ne 0 ]; then
    echo "gen baseline coverage run failed"
    exit -1
fi

for test in `ls ${DIR_UNITTEST}`; do
    echo $test
    # run unittest
    ${DIR_UNITTEST}/${test}
    if [ $? -ne 0 ]; then
        echo ${args}
        echo ${DIR_UNITTEST}/${test} "run failed"
        exit -1
    fi
done

# gen code coverage
${LCOV_CMD} -d ${DIR_GCNO} -o "${FILE_INFO_MILVUS}" -c
# merge coverage
${LCOV_CMD} -a ${FILE_INFO_BASE} -a ${FILE_INFO_MILVUS} -o "${FILE_INFO_OUTPUT}"

# remove third party from tracefiles
${LCOV_CMD} -r "${FILE_INFO_OUTPUT}" -o "${FILE_INFO_OUTPUT_NEW}" \
    "/usr/*" \
    "*/boost/*" \
    "*/cmake_build/*" \
    "*/thirdparty/*"

if [ $? -ne 0 ]; then
    echo "generate ${FILE_INFO_OUTPUT_NEW} failed"
    exit -2
fi
# gen html report
${LCOV_GEN_CMD} "${FILE_INFO_OUTPUT_NEW}" --output-directory ${DIR_LCOV_OUTPUT}/

