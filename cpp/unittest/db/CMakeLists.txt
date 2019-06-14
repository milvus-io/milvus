#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

aux_source_directory(../../src/db db_srcs)
aux_source_directory(../../src/db/scheduler db_scheduler_srcs)
aux_source_directory(../../src/config config_files)
aux_source_directory(../../src/cache cache_srcs)
aux_source_directory(../../src/wrapper wrapper_src)

include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

set(require_files
        ../../src/server/ServerConfig.cpp
        ../../src/utils/CommonUtil.cpp
        ../../src/utils/TimeRecorder.cpp
        )

set(db_test_src
    ${unittest_srcs}
    ${config_files}
    ${cache_srcs}
    ${db_srcs}
    ${db_scheduler_srcs}
    ${wrapper_src}
    ${require_files}
    MySQLMetaImpl_test.cpp
    utils.cpp
    db_tests.cpp
    meta_tests.cpp)

include_directories(/usr/include/mysql)

cuda_add_executable(db_test ${db_test_src})

set(db_libs
        faiss
        cudart
        cublas
        sqlite3
        boost_system
        boost_filesystem
        lz4
        )

target_link_libraries(db_test ${db_libs} ${unittest_libs} /usr/local/lib/libmysqlpp.so)
