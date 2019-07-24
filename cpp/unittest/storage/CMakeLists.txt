#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/s3 s3_client_src)

set(util_files
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp)

# Make sure that your call to link_directories takes place before your call to the relevant add_executable.
include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

set(s3_client_test_src
        ${unittest_srcs}
        ${s3_client_src}
        ${require_files}
        S3ClientTest.cpp
        ${MILVUS_ENGINE_SRC}/db/Status.cpp
        )

add_executable(s3_test
        ${s3_client_test_src}
        ${config_files}
        ${util_files}
        )

set(s3_client_libs
        stdc++
        aws-cpp-sdk-s3
        aws-cpp-sdk-core
        boost_filesystem_static
        )
target_link_libraries(s3_test
        ${s3_client_libs}
        ${unittest_libs}
        curl
        crypto)

install(TARGETS s3_test DESTINATION bin)