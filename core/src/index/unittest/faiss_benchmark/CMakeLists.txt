if (MILVUS_GPU_VERSION)

    include_directories(${INDEX_SOURCE_DIR}/thirdparty)
    include_directories(${INDEX_SOURCE_DIR}/include)
    include_directories(/usr/local/cuda/include)
    include_directories(/usr/local/hdf5/include)

    link_directories(/usr/local/cuda/lib64)
    link_directories(/usr/local/hdf5/lib)

    set(unittest_libs
            gtest gmock gtest_main gmock_main)

    set(depend_libs
            faiss hdf5
            )
    if (FAISS_WITH_MKL)
        set(depend_libs ${depend_libs}
                "-Wl,--start-group \
                ${MKL_LIB_PATH}/libmkl_intel_ilp64.a \
                ${MKL_LIB_PATH}/libmkl_gnu_thread.a \
                ${MKL_LIB_PATH}/libmkl_core.a \
                -Wl,--end-group -lgomp -lpthread -lm -ldl"
                )
    else ()
        set(depend_libs ${depend_libs}
                ${OpenBLAS_LIBRARIES}
                ${LAPACK_LIBRARIES}
                )
    endif ()

    set(basic_libs
            gomp gfortran pthread
            )

    include_directories(${CUDA_INCLUDE_DIRS})
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(cuda_lib
            cudart
            cublas
            )
    set(basic_libs ${basic_libs}
            ${cuda_lib}
            )

    add_executable(test_faiss_benchmark faiss_benchmark_test.cpp)
    target_link_libraries(test_faiss_benchmark ${depend_libs} ${unittest_libs} ${basic_libs})
    install(TARGETS test_faiss_benchmark DESTINATION unittest)

    add_executable(test_faiss_bitset faiss_bitset_test.cpp)
    target_link_libraries(test_faiss_bitset ${depend_libs} ${unittest_libs} ${basic_libs})
    install(TARGETS test_faiss_bitset DESTINATION unittest)
endif ()
