#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
link_directories(
        "${CMAKE_BINARY_DIR}/lib"
        "${VECWISE_THIRD_PARTY_BUILD}/lib"
)

set(unittest_srcs
        ${CMAKE_CURRENT_SOURCE_DIR}/vecwise_test.cpp ${VECWISE_THIRD_PARTY_BUILD}/include/easylogging++.cc)

set(unittest_libs
        gtest_main
        gmock_main
        pthread)

add_subdirectory(cache)
add_subdirectory(log)