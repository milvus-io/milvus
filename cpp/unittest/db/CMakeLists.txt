#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

aux_source_directory(../../src/db db_srcs)
aux_source_directory(../../src/wrapper wrapper_src)

include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")


set(db_test_src
    ${unittest_srcs}
    ${db_srcs}
    ${wrapper_src}
    db_tests.cpp)

cuda_add_executable(db_test ${db_test_src})

set(db_libs
        faiss
        cudart
        cublas
        sqlite3
        boost_system
        boost_filesystem
        )

target_link_libraries(db_test ${unittest_libs} ${db_libs})
