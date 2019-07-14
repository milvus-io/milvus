#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)
include_directories(/usr/include)

include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda/lib64)

aux_source_directory(../../src/config config_files)
aux_source_directory(../../src/cache cache_srcs)
aux_source_directory(../../src/wrapper wrapper_src)
aux_source_directory(./ test_srcs)

set(utils_srcs
        ${MILVUS_ENGINE_SRC}/utils/StringHelpFunctions.cpp
        ${MILVUS_ENGINE_SRC}/utils/TimeRecorder.cpp
        ${MILVUS_ENGINE_SRC}/utils/CommonUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/LogUtil.cpp
        )

cuda_add_executable(server_test
        ${unittest_srcs}
        ${config_files}
        ${cache_srcs}
        ${wrapper_src}
        ${test_srcs}
        ${utils_srcs}
        ${require_files}
        )

set(require_libs
        stdc++
        libgpufaiss.a
        faiss
        cudart
        cublas
        sqlite
        boost_system_static
        boost_filesystem_static
        snappy
        z
        bz2
        zstd
        lz4
        pthread
        )

target_link_libraries(server_test
        ${require_libs}
        ${cuda_library}
        ${unittest_libs}
        )

install(TARGETS server_test DESTINATION bin)

configure_file(appendix/server_config.yaml
        "${CMAKE_CURRENT_BINARY_DIR}/milvus/conf/server_config.yaml"
        COPYONLY)

configure_file(appendix/log_config.conf
        "${CMAKE_CURRENT_BINARY_DIR}/milvus/conf/log_config.conf"
        COPYONLY)
