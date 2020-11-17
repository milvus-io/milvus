#!/bin/bash

set -e

SOURCE="${BASH_SOURCE[0]}"
# resolve $SOURCE until the file is no longer a symlink
while [ -h "$SOURCE" ]; do
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

HELP="
Usage:
  $0 [flags] [Arguments]

    -i [INSTALL_PREFIX] or --install_prefix=[INSTALL_PREFIX]
                              Install directory used by install.
    -b                        Core Code build directory
    -c                        Codecov token
    -o                        Output info file name
    -h or --help              Print help information

Use \"$0 --help\" for more information about a given command.
"

ARGS=`getopt -o "i:b:c:o:h" -l "install_prefix::,help" -n "$0" -- "$@"`

eval set -- "${ARGS}"

while true ; do
    case "$1" in
        -i|--install_prefix)
            case "$2" in
                "") echo "Option install_prefix, no argument"; exit 1 ;;
                *)  INSTALL_PREFIX=$2 ; shift 2 ;;
            esac ;;
        -b)
            case "$2" in
                "") echo "Option CORE_BUILD_DIR, no argument"; exit 1 ;;
                *)  CORE_BUILD_DIR=$2 ; shift 2 ;;
            esac ;;
        -c)
            case "$2" in
                "") echo "Option CODECOV_TOKEN, no argument"; exit 1 ;;
                *)  CODECOV_TOKEN=$2 ; shift 2 ;;
            esac ;;
        -o)
            case "$2" in
                "") echo "Option OUTPUT_INFO, no argument"; exit 1 ;;
                *)  OUTPUT_INFO=$2 ; shift 2 ;;
            esac ;;
        -h|--help) echo -e "${HELP}" ; exit 0 ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

# Set defaults for vars modified by flags to this script
INSTALL_PREFIX=${INSTALL_PREFIX:="/var/lib/milvus"}
UNITTEST_DIR="${INSTALL_PREFIX}/unittest"

MILVUS_CORE_DIR="${SCRIPTS_DIR}/../../core"
CORE_BUILD_DIR=${CORE_BUILD_DIR:="${MILVUS_CORE_DIR}/cmake_build"}

LCOV_CMD="lcov"
# LCOV_GEN_CMD="genhtml"

BASE_INFO="milvus_base.info"
TEST_INFO="milvus_test.info"
TOTAL_INFO="milvus_total.info"
OUTPUT_INFO=${OUTPUT_INFO}
# LCOV_OUTPUT_DIR="lcov_out"

if [ -d ${INSTALL_PREFIX}/lib ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${INSTALL_PREFIX}/lib
fi

if [ ! -d ${UNITTEST_DIR} ]; then
	echo "The unittest folder does not exist!"
    exit 1
fi

pushd ${SCRIPTS_DIR}

# delete old code coverage info files
# rm -rf ${LCOV_OUTPUT_DIR}
rm -f ${BASE_INFO} ${TEST_INFO} ${TOTAL_INFO} ${OUTPUT_INFO}

# get baseline
${LCOV_CMD} -c -i -d ${CORE_BUILD_DIR} -o "${BASE_INFO}"
if [ $? -ne 0 ]; then
    echo "gen ${BASE_INFO} failed"
    exit 1
fi

# run unittest
for test in `ls ${UNITTEST_DIR}`; do
    if [[ ${test} == *".log" ]] || [[ ${test} == *".info" ]]; then
        echo "skip file " ${test}
        continue
    fi
    echo $test " running..."
    # run unittest
    ${UNITTEST_DIR}/${test}
    if [ $? -ne 0 ]; then
        echo ${UNITTEST_DIR}/${test} "run failed"
        exit 1
    fi
done

# gen code coverage
${LCOV_CMD} -c -d ${CORE_BUILD_DIR} -o "${TEST_INFO}"
if [ $? -ne 0 ]; then
    echo "gen ${TEST_INFO} failed"
    exit 1
fi

# merge coverage
${LCOV_CMD} -a ${BASE_INFO} -a ${TEST_INFO} -o "${TOTAL_INFO}"
if [ $? -ne 0 ]; then
    echo "gen ${TOTAL_INFO} failed"
    exit 1
fi

# remove third party from tracefiles
${LCOV_CMD} -r "${TOTAL_INFO}" -o "${OUTPUT_INFO}" \
    "/usr/*" \
    "*/cmake_build/*" \
    "*/src/index/thirdparty*" \
    "*/src/grpc*" \
    "*/thirdparty/*"

if [ $? -ne 0 ]; then
    echo "gen ${OUTPUT_INFO} failed"
    exit 1
fi

if [[ -n ${CODECOV_TOKEN} ]];then
    export CODECOV_TOKEN="${CODECOV_TOKEN}"
    curl -s https://codecov.io/bash | bash -s - -f ${OUTPUT_INFO} || echo "Codecov did not collect coverage reports"
fi

popd
