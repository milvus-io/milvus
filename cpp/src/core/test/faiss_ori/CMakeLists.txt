include_directories(${CMAKE_SOURCE_DIR}/thirdparty)
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda/lib64)
link_directories(${CMAKE_SOURCE_DIR}/thirdparty/tbb)

set(unittest_libs
        gtest gmock gtest_main gmock_main)

set(depend_libs
        faiss openblas lapack
        arrow jemalloc_pic
        tbb
        )

set(basic_libs
        cudart cublas
        gomp gfortran pthread
        )


#<GPU-TEST>
#<<<<<<<<<<
add_executable(test_gpu gpuresource_test.cpp)
target_link_libraries(test_gpu ${depend_libs} ${unittest_libs} ${basic_libs})
#<<<<<<<<<<
