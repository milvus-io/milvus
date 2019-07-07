#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

# Make sure that your call to link_directories takes place before your call to the relevant add_executable.
include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

set(validation_util_src
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.h)

set(validation_util_test_src
        ${unittest_srcs}
        ${validation_util_src}
        ${require_files}
        ValidationUtilTest.cpp
        )

add_executable(valication_util_test
        ${validation_util_test_src}
        ${config_files})

target_link_libraries(valication_util_test
        ${unittest_libs}
        boost_filesystem)

install(TARGETS valication_util_test DESTINATION bin)