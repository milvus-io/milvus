#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)
include_directories(/usr/include)

aux_source_directory(../../src/config config_files)
aux_source_directory(../../src/cache cache_srcs)
aux_source_directory(./ test_srcs)

set(require_files
        ../../src/server/VecIdMapper.cpp
        ../../src/server/ServerConfig.cpp
        ../../src/utils/CommonUtil.cpp
        )

add_executable(server_test
        ${unittest_srcs}
        ${config_files}
        ${cache_srcs}
        ${test_srcs}
        ${require_files}
        )

set(require_libs
        yaml-cpp
        boost_system
        boost_filesystem
        pthread
        snappy
        bz2
        z
        rocksdb
        )

link_libraries(/usr/lib/x86_64-linux-gnu)

target_link_libraries(server_test
        ${unittest_libs}
        ${require_libs}
        )
