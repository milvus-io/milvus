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

include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/NGT/lib)
include_directories(${INDEX_SOURCE_DIR}/knowhere)
include_directories(${INDEX_SOURCE_DIR})

set(DEPEND_LIBS
        gtest
        gmock
        gtest_main
        gmock_main
        faiss
        fiu
        ngt
        index_log
        openblas
        )

set(BASIC_LIBS gomp gfortran pthread)

set(UTIL_SRCS
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissIO.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/IndexParameter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/DynamicResultSet.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/Statistics.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/IndexType.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/Exception.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/Timer.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/Utils.cpp
        ${INDEX_SOURCE_DIR}/unittest/utils.cpp
        )

if (MILVUS_GPU_VERSION)
    include_directories(${CUDA_INCLUDE_DIRS})
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(CUDA_LIB
            cudart
            cublas
            )
    set(BASIC_LIBS ${basic_libs} ${cuda_lib})
    set(UTIL_SRCS ${util_srcs}
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissGpuResourceMgr.cpp
            )
endif ()

set(FAISS_SRCS
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/FaissBaseIndex.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/FaissBaseBinaryIndex.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexBinaryIDMAP.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexBinaryIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIDMAP.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFPQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFHNSW.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_offset_index/OffsetBaseIndex.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_offset_index/IndexIVF_NM.cpp
        )

if (MILVUS_GPU_VERSION)
    set(FAISS_SRCS ${faiss_srcs}
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/ConfAdapter.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/Cloner.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIDMAP.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIVF.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIVFSQ.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIVFPQ.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexIVFSQHybrid.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_offset_index/gpu/IndexGPUIVF_NM.cpp
            )
endif ()

################################################################################
#<NSG-TEST>
add_definitions(-std=c++11 -O3 -march=native -Werror -DINFO)

find_package(OpenMP REQUIRED)
if (OpenMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
else ()
    message(FATAL_ERROR "no OpenMP supprot")
endif ()

set(ALL_INDEXING_SRCS
        ${UTIL_SRCS}
        test_instructionset.cpp
        test_common.cpp
        test_customized_index.cpp
        test_idmap.cpp
        test_ivf.cpp
        test_ivf_hnsw.cpp
        test_ivf_cpu_nm.cpp
        test_binaryidmap.cpp
        test_binaryivf.cpp
        test_nsg.cpp
        test_hnsw.cpp
        test_rhnsw_flat.cpp
        test_rhnsw_pq.cpp
        test_rhnsw_sq8.cpp
        test_structured_index_sort.cpp
        test_structured_index_flat.cpp
        test_ngtpanng.cpp
        test_ngtonng.cpp
        )

if (MILVUS_GPU_VERSION)
    set(ALL_INDEXING_SRCS
            ${ALL_INDEXING_SRCS}
            test_ivf_gpu_nm.cpp
            test_gpuresource.cpp
            )
endif ()

if (MILVUS_SUPPORT_SPTAG)
    set(ALL_INDEXING_SRCS
            test_sptag.cpp
            )
endif()

add_executable(test_knowhere ${ALL_INDEXING_SRCS})
target_link_libraries(test_knowhere knowhere ${DEPEND_LIBS})

install(TARGETS test_knowhere)
