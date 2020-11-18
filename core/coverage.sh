#!/bin/bash

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/milvus/lib

LCOV_CMD="lcov"
LCOV_GEN_CMD="genhtml"

BASE_INFO="milvus_base.info"
TEST_INFO="milvus_test.info"
TOTAL_INFO="milvus_total.info"
OUTPUT_INFO="milvus_output.info"
LCOV_OUTPUT_DIR="lcov_out"

GCNO_DIR="cmake_build"
UNITTEST_DIR="milvus/unittest"

# delete old code coverage info files
rm -rf ${LCOV_OUTPUT_DIR}
rm -f ${BASE_INFO} ${TEST_INFO} ${TOTAL_INFO} ${OUTPUT_INFO}

# get baseline
${LCOV_CMD} -c -i -d ${GCNO_DIR} -o "${BASE_INFO}"
if [ $? -ne 0 ]; then
    echo "generate ${BASE_INFO} failed"
    exit 1
fi

# run unittest
for test in `ls ${UNITTEST_DIR}`; do
    if [[ ${test} == *".log" ]] || [[ ${test} == *".info" ]]; then
        echo "skip file ${test}"
        continue
    fi
    echo $test "running..."
    # run unittest
    ./${UNITTEST_DIR}/${test}
    if [ $? -ne 0 ]; then
        echo ${UNITTEST_DIR}/${test} "run failed"
        exit 1
    fi
done

# gen code coverage
${LCOV_CMD} -c -d ${GCNO_DIR} -o "${TEST_INFO}"
if [ $? -ne 0 ]; then
    echo "generate ${TEST_INFO} failed"
    exit 1
fi

# merge coverage
${LCOV_CMD} -a ${BASE_INFO} -a ${TEST_INFO} -o "${TOTAL_INFO}"
if [ $? -ne 0 ]; then
    echo "generate ${TOTAL_INFO} failed"
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
    echo "generate ${OUTPUT_INFO} failed"
    exit 1
fi

# gen html report
${LCOV_GEN_CMD} "${OUTPUT_INFO}" --output-directory ${LCOV_OUTPUT_DIR}
