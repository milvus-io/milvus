#!/bin/bash

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/milvus/lib

LCOV_CMD="lcov"
LCOV_GEN_CMD="genhtml"

FILE_INFO_BASE="base.info"
FILE_INFO_MILVUS="server.info"
FILE_INFO_OUTPUT="output.info"
FILE_INFO_OUTPUT_NEW="output_new.info"
DIR_LCOV_OUTPUT="lcov_out"

DIR_GCNO="cmake_build"
DIR_UNITTEST="milvus/bin"
 
MYSQL_USER_NAME=root
MYSQL_PASSWORD=Fantast1c
MYSQL_HOST='192.168.1.194'
MYSQL_PORT='3306'

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

# get baseline
${LCOV_CMD} -c -i -d ${DIR_GCNO} -o "${FILE_INFO_BASE}"
if [ $? -ne 0 ]; then
    echo "gen baseline coverage run failed"
    exit -1
fi

for test in `ls ${DIR_UNITTEST}`; do
    case ${test} in
        db_test)
            # set run args for db_test
            args="mysql://${MYSQL_USER_NAME}:${MYSQL_PASSWORD}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB_NAME}"
            ;;
        *_test)
            args=""
            ;;
    esac
    # run unittest
    ./${DIR_UNITTEST}/${test} "${args}"
    if [ $? -ne 0 ]; then
        echo ${DIR_UNITTEST}/${test} "run failed"
    fi
done

# gen test converage
${LCOV_CMD} -d ${DIR_GCNO} -o "${FILE_INFO_MILVUS}" -c
# merge coverage
${LCOV_CMD} -a ${FILE_INFO_BASE} -a ${FILE_INFO_MILVUS} -o "${FILE_INFO_OUTPUT}"

# remove third party from tracefiles
${LCOV_CMD} -r "${FILE_INFO_OUTPUT}" -o "${FILE_INFO_OUTPUT_NEW}" \
    "/usr/*" \
    "*/boost/*" \
    "*/cmake_build/*_ep-prefix/*" \

# gen html report
${LCOV_GEN_CMD} "${FILE_INFO_OUTPUT_NEW}" --output-directory ${DIR_LCOV_OUTPUT}/
