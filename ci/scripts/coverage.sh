#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

MILVUS_CORE_DIR="${SCRIPTS_DIR}/../../core"
CORE_BUILD_DIR="${MILVUS_CORE_DIR}/cmake_build"
CODECOV_TOKEN=""

while getopts "b:c:h" arg
do
        case $arg in
             b)
                CORE_BUILD_DIR=$OPTARG # CORE_BUILD_DIR
                ;;
             c)
                CODECOV_TOKEN=$OPTARG
                ;;
             h) # help
                echo "

parameter:
-b: core code build directory
-c: codecov token
-h: help

usage:
./coverage.sh -b \${CORE_BUILD_DIR} -c \${CODECOV_TOKEN} [-h]
                "
                exit 0
                ;;
             ?)
                echo "ERROR! unknown argument"
        exit 1
        ;;
        esac
done

LCOV_CMD="lcov"
# LCOV_GEN_CMD="genhtml"

FILE_INFO_BASE="base.info"
FILE_INFO_MILVUS="server.info"
FILE_INFO_OUTPUT="output.info"
FILE_INFO_OUTPUT_NEW="output_new.info"
DIR_LCOV_OUTPUT="lcov_out"

DIR_GCNO="${CORE_BUILD_DIR}"
DIR_UNITTEST="${INSTALL_PREFIX}/unittest"

cd ${SCRIPTS_DIR}

# delete old code coverage info files
rm -rf ${DIR_LCOV_OUTPUT}
rm -f ${FILE_INFO_BASE} ${FILE_INFO_MILVUS} ${FILE_INFO_OUTPUT} ${FILE_INFO_OUTPUT_NEW}

# get baseline
${LCOV_CMD} -c -i -d ${DIR_GCNO} -o "${FILE_INFO_BASE}"
if [ $? -ne 0 ]; then
    echo "gen baseline coverage run failed"
    exit -1
fi

# gen code coverage
${LCOV_CMD} -d ${DIR_GCNO} -o "${FILE_INFO_MILVUS}" -c
# merge coverage
${LCOV_CMD} -a ${FILE_INFO_BASE} -a ${FILE_INFO_MILVUS} -o "${FILE_INFO_OUTPUT}"

# remove third party from tracefiles
${LCOV_CMD} -r "${FILE_INFO_OUTPUT}" -o "${FILE_INFO_OUTPUT_NEW}" \
    "/usr/*" \
    "*/boost/*" \
    "*/cmake_build/*_ep-prefix/*" \
    "*/src/index/cmake_build*" \
    "*/src/index/thirdparty*" \
    "*/src/grpc*" \
    "*/src/metrics/MetricBase.h" \
    "*/src/server/Server.cpp" \
    "*/src/server/DBWrapper.cpp" \
    "*/src/server/grpc_impl/GrpcServer.cpp" \
    "*/thirdparty/*"

if [ $? -ne 0 ]; then
    echo "gen ${FILE_INFO_OUTPUT_NEW} failed"
    exit 2
fi

if [[ ! -z ${CODECOV_TOKEN} ]];then
    export CODECOV_TOKEN="${CODECOV_TOKEN}"
    curl -s https://codecov.io/bash | bash -s - -f output_new.info || echo "Codecov did not collect coverage reports"
fi
