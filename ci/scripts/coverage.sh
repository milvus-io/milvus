#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

INSTALL_PREFIX="/var/lib/milvus"
MILVUS_CORE_DIR="${SCRIPTS_DIR}/../../core"
CORE_BUILD_DIR="${MILVUS_CORE_DIR}/cmake_build"
MYSQL_USER_NAME=root
MYSQL_PASSWORD=123456
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="3306"
CODECOV_TOKEN=""

while getopts "o:b:u:p:t:c:h" arg
do
        case $arg in
             o)
                INSTALL_PREFIX=$OPTARG
                ;;
             b)
                CORE_BUILD_DIR=$OPTARG # CORE_BUILD_DIR
                ;;
             u)
                MYSQL_USER_NAME=$OPTARG
                ;;
             p)
                MYSQL_PASSWORD=$OPTARG
                ;;
             t)
                MYSQL_HOST=$OPTARG
                ;;
             c)
                CODECOV_TOKEN=$OPTARG
                ;;
             h) # help
                echo "

parameter:
-o: milvus install prefix(default: /var/lib/milvus)
-b: core code build directory
-u: mysql account
-p: mysql password
-t: mysql host
-c: codecov token
-h: help

usage:
./coverage.sh -o \${INSTALL_PREFIX} -b \${CORE_BUILD_DIR} -u \${MYSQL_USER} -p \${MYSQL_PASSWORD} -t \${MYSQL_HOST} -c \${CODECOV_TOKEN} [-h]
                "
                exit 0
                ;;
             ?)
                echo "ERROR! unknown argument"
        exit 1
        ;;
        esac
done

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${INSTALL_PREFIX}/lib

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

MYSQL_DB_NAME=milvus_`date +%s%N`

function mysql_exc()
{
    cmd=$1
    mysql -h${MYSQL_HOST} -u${MYSQL_USER_NAME} -p${MYSQL_PASSWORD} -e "${cmd}"
    if [ $? -ne 0 ]; then
        echo "mysql $cmd run failed"
    fi
}

mysql_exc "CREATE DATABASE IF NOT EXISTS ${MYSQL_DB_NAME};"
mysql_exc "GRANT ALL PRIVILEGES ON ${MYSQL_DB_NAME}.* TO '${MYSQL_USER_NAME}'@'%';"
mysql_exc "FLUSH PRIVILEGES;"
mysql_exc "USE ${MYSQL_DB_NAME};"

# get baseline
${LCOV_CMD} -c -i -d ${DIR_GCNO} -o "${FILE_INFO_BASE}"
if [ $? -ne 0 ]; then
    echo "gen baseline coverage run failed"
    exit -1
fi

for test in `ls ${DIR_UNITTEST}`; do
    echo $test
    case ${test} in
        test_db)
            # set run args for test_db
            args="mysql://${MYSQL_USER_NAME}:${MYSQL_PASSWORD}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB_NAME}"
            ;;
        *test_*)
            args=""
            ;;
    esac
    # run unittest
    ${DIR_UNITTEST}/${test} "${args}"
    if [ $? -ne 0 ]; then
        echo ${args}
        echo ${DIR_UNITTEST}/${test} "run failed"
        exit 1
    fi
done

mysql_exc "DROP DATABASE IF EXISTS ${MYSQL_DB_NAME};"

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
