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
DIR_UNITTEST="milvus/unittest"

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
    ./${DIR_UNITTEST}/${test}
    if [ $? -ne 0 ]; then
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
    "*/cmake_build/*" \
    "*/src/index/thirdparty*" \
    "*/src/grpc*" \
    "*/thirdparty/*"

if [ $? -ne 0 ]; then
    echo "generate ${FILE_INFO_OUTPUT_NEW} failed"
    exit -2
fi
# gen html report
${LCOV_GEN_CMD} "${FILE_INFO_OUTPUT_NEW}" --output-directory ${DIR_LCOV_OUTPUT}/
