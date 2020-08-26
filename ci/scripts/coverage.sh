#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

HELP="
Usage:
  $0 [flags] [Arguments]

    -b                                Core Code build directory
    -c                                Codecov token
    -h or --help                      Print help information


Use \"$0  --help\" for more information about a given command.
"

ARGS=`getopt -o "b:c:h" -l "help" -n "$0" -- "$@"`

eval set -- "${ARGS}"

while true ; do
        case "$1" in
                -b)
                        # o has an optional argument. As we are in quoted mode,
                        # an empty parameter will be generated if its optional
                        # argument is not found.
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

if [ $? -ne 0 ]; then
    echo "gen ${FILE_INFO_MILVUS} failed"
    exit 0
fi

# merge coverage
${LCOV_CMD} -a ${FILE_INFO_BASE} -a ${FILE_INFO_MILVUS} -o "${FILE_INFO_OUTPUT}"

# remove third party from tracefiles
${LCOV_CMD} -r "${FILE_INFO_OUTPUT}" -o "${FILE_INFO_OUTPUT_NEW}" \
    "/usr/*" \
    "*/cmake_build/*" \
    "*/src/index/thirdparty*" \
    "*/src/grpc*" \
    "*/thirdparty/*"

if [ $? -ne 0 ]; then
    echo "gen ${FILE_INFO_OUTPUT_NEW} failed"
    exit 0
fi

if [[ -n ${CODECOV_TOKEN} ]];then
    export CODECOV_TOKEN="${CODECOV_TOKEN}"
    curl -s https://codecov.io/bash | bash -s - -f output_new.info || echo "Codecov did not collect coverage reports"
fi
