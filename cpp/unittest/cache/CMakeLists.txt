#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

aux_source_directory(../../src/cache cache_srcs)

set(cache_test_src
        ${unittest_srcs}
        ${cache_srcs}
        cache_test.cpp)

add_executable(cache_test ${cache_test_src})

target_link_libraries(cache_test ${unittest_libs})
