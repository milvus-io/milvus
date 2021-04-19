enable_testing()
find_package(GTest REQUIRED)

enable_testing()

set(unittest_srcs ${CMAKE_CURRENT_SOURCE_DIR}/unittest_entry.cpp)

set(unittest_libs
        milvus_sdk
        gtest
        pthread
        )

