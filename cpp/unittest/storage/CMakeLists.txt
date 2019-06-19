#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/s3 s3_client_src)

# Make sure that your call to link_directories takes place before your call to the relevant add_executable.
include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

set(s3_client_test_src
        ${unittest_srcs}
        ${s3_client_src}
        ${require_files}
        S3ClientTest.cpp
        ${MILVUS_ENGINE_SRC}/db/Status.cpp
        )

add_executable(s3_test
        ${s3_client_test_src}
        ${config_files})

set(s3_client_libs
        stdc++
        libaws-cpp-sdk-s3.a
        libaws-cpp-sdk-core.a
        libaws-c-event-stream.a
        libaws-checksums.a
        libaws-c-common.a
        boost_filesystem
        )
target_link_libraries(s3_test
        ${s3_client_libs}
        ${unittest_libs}
        curl
        crypto)

install(TARGETS s3_test DESTINATION bin)