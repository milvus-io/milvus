if (KNOWHERE_GPU_VERSION)

    include_directories(${INDEX_SOURCE_DIR}/thirdparty)
    include_directories(${INDEX_SOURCE_DIR}/include)

    set(unittest_libs
            gtest gmock gtest_main gmock_main)

    set(depend_libs
            faiss
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

    #<GPU-TEST>
    if (NOT TARGET test_gpu)
        add_executable(test_gpu gpuresource_test.cpp)
    endif ()
    target_link_libraries(test_gpu ${depend_libs} ${unittest_libs} ${basic_libs})

    install(TARGETS test_gpu DESTINATION unittest)

endif ()
