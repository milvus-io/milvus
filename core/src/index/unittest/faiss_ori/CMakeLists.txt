include_directories(${CORE_SOURCE_DIR}/thirdparty)
include_directories(${CORE_SOURCE_DIR}/include)
include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda/lib64)
link_directories(${CORE_SOURCE_DIR}/thirdparty/tbb)

set(unittest_libs
        gtest gmock gtest_main gmock_main)

set(depend_libs
        faiss openblas lapack
        arrow ${ARROW_PREFIX}/lib/libjemalloc_pic.a
        tbb
        )

set(basic_libs
        cudart cublas
        gomp gfortran pthread
        )


#<GPU-TEST>
if(NOT TARGET test_gpu)
    add_executable(test_gpu gpuresource_test.cpp)
endif()
target_link_libraries(test_gpu ${depend_libs} ${unittest_libs} ${basic_libs})

install(TARGETS test_gpu DESTINATION unittest)