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
        pthread
        wrapper
        parquet
        )

install(TARGETS wrapper_test DESTINATION ${CMAKE_INSTALL_PREFIX})
