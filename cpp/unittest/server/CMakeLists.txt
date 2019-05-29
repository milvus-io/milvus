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

set(require_files
        ../../src/server/VecIdMapper.cpp
        ../../src/server/RocksIdMapper.cpp
        ../../src/server/ServerConfig.cpp
        ../../src/utils/CommonUtil.cpp
        ../../src/utils/TimeRecorder.cpp
        ../../src/utils/StringHelpFunctions.cpp
        ../../src/utils/AttributeSerializer.cpp
        )

cuda_add_executable(server_test
        ${unittest_srcs}
        ${config_files}
        ${cache_srcs}
        ${wrapper_src}
        ${test_srcs}
        ${require_files}
        )

set(require_libs
        stdc++
        rocksdb
        faiss
        cudart
        cublas
        sqlite3
        boost_system
        boost_filesystem
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
