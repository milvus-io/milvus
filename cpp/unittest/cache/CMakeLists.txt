#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

set(cache_srcs
        ../../src/cache/Cache.cpp
        ../../src/cache/CacheMgr.cpp)

set(cache_test_src
        ${unittest_srcs}
        ${cache_srcs}
        cache_tests.cpp)

add_executable(cache_tests ${cache_test_src})

target_link_libraries(cache_tests ${unittest_libs})
