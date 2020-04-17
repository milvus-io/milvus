include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${INDEX_SOURCE_DIR}/knowhere)
include_directories(${INDEX_SOURCE_DIR})

set(depend_libs
        gtest gmock gtest_main gmock_main
        faiss fiu
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
            ${BLAS_LIBRARIES}
            ${LAPACK_LIBRARIES}
            )
endif ()

set(basic_libs
        gomp gfortran pthread
        )

set(util_srcs
        ${MILVUS_THIRDPARTY_SRC}/easyloggingpp/easylogging++.cc
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissIO.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/IndexParameter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexType.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/Exception.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/Timer.cpp
        ${INDEX_SOURCE_DIR}/unittest/utils.cpp
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
    set(util_srcs ${util_srcs}
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissGpuResourceMgr.cpp
            )
endif ()

set(faiss_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/FaissBaseIndex.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/FaissBaseBinaryIndex.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexBinaryIDMAP.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexBinaryIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIDMAP.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFPQ.cpp
        )
if (KNOWHERE_GPU_VERSION)
set(faiss_srcs ${faiss_srcs}
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/Cloner.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIDMAP.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIVFSQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexGPUIVFPQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/gpu/IndexIVFSQHybrid.cpp
        )
endif ()

################################################################################
#<INSTRUCTIONSET-TEST>
if (NOT TARGET test_instructionset)
    add_executable(test_instructionset test_instructionset.cpp)
endif ()
target_link_libraries(test_instructionset ${depend_libs} ${unittest_libs})

################################################################################
#<KNOWHERE-COMMON-TEST>
if (NOT TARGET test_knowhere_common)
    add_executable(test_knowhere_common test_common.cpp ${util_srcs})
endif ()
target_link_libraries(test_knowhere_common ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_knowhere_common DESTINATION unittest)

if (KNOWHERE_GPU_VERSION)
################################################################################
#<GPU-RESOURCE-TEST>
add_executable(test_gpuresource test_gpuresource.cpp ${util_srcs} ${faiss_srcs})
target_link_libraries(test_gpuresource ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_gpuresource DESTINATION unittest)

################################################################################
#<CUSTOMIZED-INDEX-TEST>
add_executable(test_customized_index test_customized_index.cpp ${util_srcs} ${faiss_srcs})
target_link_libraries(test_customized_index ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_customized_index DESTINATION unittest)
endif ()

################################################################################
#<IDMAP-TEST>
if (NOT TARGET test_idmap)
    add_executable(test_idmap test_idmap.cpp ${faiss_srcs} ${util_srcs})
endif ()
target_link_libraries(test_idmap ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_idmap DESTINATION unittest)

################################################################################
#<IVF-TEST>
if (NOT TARGET test_ivf)
    add_executable(test_ivf test_ivf.cpp ${faiss_srcs} ${util_srcs})
endif ()
target_link_libraries(test_ivf ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_ivf DESTINATION unittest)

################################################################################
#<BinaryIDMAP-TEST>
if (NOT TARGET test_binaryidmap)
    add_executable(test_binaryidmap test_binaryidmap.cpp ${faiss_srcs} ${util_srcs})
endif ()
target_link_libraries(test_binaryidmap ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_binaryidmap DESTINATION unittest)

################################################################################
#<BinaryIVF-TEST>
if (NOT TARGET test_binaryivf)
    add_executable(test_binaryivf test_binaryivf.cpp ${faiss_srcs} ${util_srcs})
endif ()
target_link_libraries(test_binaryivf ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_binaryivf DESTINATION unittest)


################################################################################
#<NSG-TEST>
add_definitions(-std=c++11 -O3 -lboost -march=native -Wall -DINFO)

find_package(OpenMP REQUIRED)
if (OpenMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
else ()
    message(FATAL_ERROR "no OpenMP supprot")
endif ()

include_directories(${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/impl/nsg)
aux_source_directory(${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/impl/nsg nsg_src)
set(interface_src
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexNSG.cpp
        )
if (NOT TARGET test_nsg)
    add_executable(test_nsg test_nsg.cpp ${interface_src} ${nsg_src} ${util_srcs} ${faiss_srcs})
endif ()
target_link_libraries(test_nsg ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_nsg DESTINATION unittest)

################################################################################
#<HNSW-TEST>
set(hnsw_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexHNSW.cpp
        )
if (NOT TARGET test_hnsw)
    add_executable(test_hnsw test_hnsw.cpp ${hnsw_srcs} ${util_srcs})
endif ()
target_link_libraries(test_hnsw ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_hnsw DESTINATION unittest)

################################################################################
#<SPTAG-TEST>
set(sptag_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/adapter/SptagAdapter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/SPTAGParameterMgr.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexSPTAG.cpp
        )
if (NOT TARGET test_sptag)
    add_executable(test_sptag test_sptag.cpp ${sptag_srcs} ${util_srcs})
endif ()
target_link_libraries(test_sptag
        SPTAGLibStatic
        ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_sptag DESTINATION unittest)

################################################################################
#<ANNOY-TEST>
set(annoy_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexAnnoy.cpp
        )
if (NOT TARGET test_annoy)
    add_executable(test_annoy test_annoy.cpp ${annoy_srcs} ${util_srcs})
endif ()
target_link_libraries(test_annoy ${depend_libs} ${unittest_libs} ${basic_libs})
install(TARGETS test_annoy DESTINATION unittest)


#add_subdirectory(faiss_ori)
#add_subdirectory(faiss_benchmark)
#add_subdirectory(metric_alg_benchmark)
