#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)
include_directories(/usr/include)

include_directories(/usr/local/cuda/include)

aux_source_directory(../../src/db db_srcs)
aux_source_directory(../../src/config config_files)
aux_source_directory(../../src/cache cache_srcs)
aux_source_directory(../../src/wrapper wrapper_src)

link_directories("/usr/local/cuda/lib64")

set(require_files
        ../../src/license/License.cpp
        )

cuda_add_executable(license_test
        ${unittest_srcs}
        ${config_files}
        ${cache_srcs}
        ${db_srcs}
        ${require_files}
        )

set(require_libs
        libvecwise_engine.a
        stdc++
        boost_system
        boost_filesystem
        pthread
        snappy
        bz2
        z
        zstd
        rocksdb
        faiss
        cudart
        cublas
        sqlite3
        lz4
        crypto
        )

target_link_libraries(license_test
        ${unittest_libs}
        ${require_libs}
        ${cuda_library}
        )