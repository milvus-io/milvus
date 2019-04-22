#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

aux_source_directory(../../src/cache cache_srcs)
aux_source_directory(./ test_srcs)

add_executable(server_test
        ${unittest_srcs}
        ${cache_srcs}
        ${test_srcs}
        ../../src/server/VecIdMapper.cpp
        )

target_link_libraries(server_test ${unittest_libs})
