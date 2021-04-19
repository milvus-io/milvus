add_executable(wrapper_test
        ParquetWrapperTest.cpp)

include(FetchContent)
FetchContent_Declare(googletest
        URL "https://github.com/google/googletest/archive/release-1.10.0.tar.gz")
set(BUILD_GMOCK CACHE BOOL OFF)
set(INSTALL_GTEST CACHE BOOL OFF)
FetchContent_MakeAvailable(googletest)

target_link_libraries(wrapper_test
        gtest_main
        wrapper
        parquet arrow thrift utf8proc pthread
        )

# Defines `gtest_discover_tests()`.
#include(GoogleTest)
#gtest_discover_tests(milvusd_test)