#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

aux_source_directory(../../src/wrapper wrapper_src)
aux_source_directory(../../src/config config_files)

# Make sure that your call to link_directories takes place before your call to the relevant add_executable.
include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

set(require_files
        ../../src/server/VecIdMapper.cpp
        ../../src/server/ServerConfig.cpp
        ../../src/utils/CommonUtil.cpp
        ../../src/utils/TimeRecorder.cpp
        )

set(wrapper_test_src
        ${unittest_srcs}
        ${wrapper_src}
        ${config_files}
        ${require_files}
        wrapper_test.cpp
        )

add_executable(wrapper_test ${wrapper_test_src})

set(wrapper_libs
        stdc++
        boost_system
        boost_filesystem
        faiss
        cudart
        cublas
        sqlite3
        rocksdb
        snappy
        bz2
        z
        zstd
        )
target_link_libraries(wrapper_test ${unittest_libs} ${wrapper_libs})

set(topk_test_src
        topk_test.cpp
        ${CMAKE_SOURCE_DIR}/src/wrapper/Topk.cu)

cuda_add_executable(topk_test ${topk_test_src})
target_link_libraries(topk_test ${unittest_libs} ${faiss_libs})
