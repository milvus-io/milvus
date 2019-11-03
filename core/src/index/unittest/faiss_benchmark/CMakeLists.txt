include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/include)
include_directories(/usr/local/cuda/include)
include_directories(/usr/local/hdf5/include)

link_directories(/usr/local/cuda/lib64)
link_directories(/usr/local/hdf5/lib)

set(unittest_libs
        gtest gmock gtest_main gmock_main)

set(depend_libs
        faiss openblas lapack hdf5
        arrow ${ARROW_PREFIX}/lib/libjemalloc_pic.a
        )

set(basic_libs
        cudart cublas
        gomp gfortran pthread
        )

add_executable(test_faiss_benchmark faiss_benchmark_test.cpp)
target_link_libraries(test_faiss_benchmark ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_faiss_benchmark DESTINATION unittest)
