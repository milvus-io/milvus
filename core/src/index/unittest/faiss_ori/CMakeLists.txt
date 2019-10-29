include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/include)

set(unittest_libs
        gtest gmock gtest_main gmock_main)

set(depend_libs
        faiss openblas lapack
        arrow ${ARROW_PREFIX}/lib/libjemalloc_pic.a
        )

set(basic_libs
        gomp gfortran pthread
        )

if (KNOWHERE_GPU_VERSION)
    include_directories(${CUDA_INCLUDE_DIRS})
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(cuda_lib
            cudart
            cublas
            )
    set(basic_libs ${basic_libs}
            ${cuda_lib}
            )
endif ()

#<GPU-TEST>
if (NOT TARGET test_gpu)
    add_executable(test_gpu gpuresource_test.cpp)
endif ()
target_link_libraries(test_gpu ${depend_libs} ${unittest_libs} ${basic_libs})

install(TARGETS test_gpu DESTINATION unittest)