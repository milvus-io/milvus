#!/bin/bash

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/milvus/lib

MYSQL_USER_NAME=root
MYSQL_PASSWORD=123456
MYSQL_HOST='127.0.0.1'
MYSQL_PORT='3306'

while getopts "u:p:t:h" arg
do
        case $arg in
             u)
                MYSQL_USER_NAME=$OPTARG
                ;;
             p)
                MYSQL_PASSWORD=$OPTARG
                ;;
             t)
                MYSQL_HOST=$OPTARG
                ;;
             h) # help
                echo "

parameter:
-u: mysql account
-p: mysql password
-t: mysql host
-h: help

usage:
./coverage.sh -u \${MYSQL_USER} -p \${MYSQL_PASSWORD} -t \${MYSQL_HOST} [-h]
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
LCOV_GEN_CMD="genhtml"

FILE_INFO_BASE="base.info"
FILE_INFO_MILVUS="server.info"
FILE_INFO_OUTPUT="output.info"
FILE_INFO_OUTPUT_NEW="output_new.info"
DIR_LCOV_OUTPUT="lcov_out"

DIR_GCNO="cmake_build"
DIR_UNITTEST="milvus/unittest"

# delete old code coverage info files
rm -f FILE_INFO_BASE
rm -f FILE_INFO_MILVUS
rm -f FILE_INFO_OUTPUT
rm -f FILE_INFO_OUTPUT_NEW
rm -rf lcov_out
rm -f FILE_INFO_BASE FILE_INFO_MILVUS FILE_INFO_OUTPUT FILE_INFO_OUTPUT_NEW

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
    ./${DIR_UNITTEST}/${test} "${args}"
    if [ $? -ne 0 ]; then
        echo ${args}
        echo ${DIR_UNITTEST}/${test} "run failed"
        exit -1
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
    echo "generate ${FILE_INFO_OUTPUT_NEW} failed"
    exit -2
fi
# gen html report
${LCOV_GEN_CMD} "${FILE_INFO_OUTPUT_NEW}" --output-directory ${DIR_LCOV_OUTPUT}/
