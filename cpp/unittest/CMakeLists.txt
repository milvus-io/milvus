#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
link_directories(
        "${CMAKE_BINARY_DIR}/lib"
        "${GTEST_LIBRARY_DIR}"
)

set(unittest_srcs
        ${CMAKE_CURRENT_SOURCE_DIR}/vecwise_test.cpp)

set(unittest_libs
        gtest
        gmock
        pthread)

add_subdirectory(log)