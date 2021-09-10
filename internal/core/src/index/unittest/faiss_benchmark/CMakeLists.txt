# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

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
