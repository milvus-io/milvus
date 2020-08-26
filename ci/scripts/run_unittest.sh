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
    -h or --help              Print help information

Use \"$0 --help\" for more information about a given command.
"

ARGS=`getopt -o "i:b:c:h" -l "install_prefix::,help" -n "$0" -- "$@"`

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
                -h|--help) echo -e "${HELP}" ; exit 0 ;;
                --) shift ; break ;;
                *) echo "Internal error!" ; exit 1 ;;
        esac
done

# Set defaults for vars modified by flags to this script
INSTALL_PREFIX=${INSTALL_PREFIX:="/var/lib/milvus"}
DIR_UNITTEST="${INSTALL_PREFIX}/unittest"

MILVUS_CORE_DIR="${SCRIPTS_DIR}/../../core"
CORE_BUILD_DIR=${CORE_BUILD_DIR:="${MILVUS_CORE_DIR}/cmake_build"}

LCOV_CMD="lcov"
# LCOV_GEN_CMD="genhtml"

FILE_INFO_BASE="base.info"
FILE_INFO_MILVUS="server.info"
FILE_INFO_OUTPUT="output.info"
FILE_INFO_OUTPUT_NEW="output_new.info"
DIR_LCOV_OUTPUT="lcov_out"

DIR_GCNO="${CORE_BUILD_DIR}"
DIR_UNITTEST="${INSTALL_PREFIX}/unittest"

if [ -d ${INSTALL_PREFIX}/lib ]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${INSTALL_PREFIX}/lib
fi

if [ ! -d ${DIR_UNITTEST} ]; then
	echo "The unittest folder does not exist!"
    exit 1
fi

pushd ${SCRIPTS_DIR}

# delete old code coverage info files
rm -rf ${DIR_LCOV_OUTPUT}
rm -f ${FILE_INFO_BASE} ${FILE_INFO_MILVUS} ${FILE_INFO_OUTPUT} ${FILE_INFO_OUTPUT_NEW}

# get baseline
${LCOV_CMD} -c -i -d ${DIR_GCNO} -o "${FILE_INFO_BASE}"
if [ $? -ne 0 ]; then
    echo "gen ${FILE_INFO_BASE} failed"
    exit 1
fi

# run unittest
for test in `ls ${DIR_UNITTEST}`; do
    echo "running ... " $test
    # run unittest
    ${DIR_UNITTEST}/${test}
    if [ $? -ne 0 ]; then
        echo ${DIR_UNITTEST}/${test} "run failed"
        exit 1
    fi
done

# gen code coverage
${LCOV_CMD} -c -d ${DIR_GCNO} -o "${FILE_INFO_MILVUS}"
if [ $? -ne 0 ]; then
    echo "gen ${FILE_INFO_MILVUS} failed"
    exit 1
fi

# merge coverage
${LCOV_CMD} -a ${FILE_INFO_BASE} -a ${FILE_INFO_MILVUS} -o "${FILE_INFO_OUTPUT}"
if [ $? -ne 0 ]; then
    echo "gen ${FILE_INFO_OUTPUT} failed"
    exit 1
fi

# remove third party from tracefiles
${LCOV_CMD} -r "${FILE_INFO_OUTPUT}" -o "${FILE_INFO_OUTPUT_NEW}" \
    "/usr/*" \
    "*/cmake_build/*" \
    "*/src/index/thirdparty*" \
    "*/src/grpc*" \
    "*/thirdparty/*"

if [ $? -ne 0 ]; then
    echo "gen ${FILE_INFO_OUTPUT_NEW} failed"
    exit 1
fi

if [[ -n ${CODECOV_TOKEN} ]];then
    export CODECOV_TOKEN="${CODECOV_TOKEN}"
    curl -s https://codecov.io/bash | bash -s - -f ${FILE_INFO_OUTPUT_NEW} || echo "Codecov did not collect coverage reports"
fi

popd
