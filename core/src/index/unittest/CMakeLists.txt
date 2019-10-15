include_directories(${CORE_SOURCE_DIR}/thirdparty)
include_directories(${CORE_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${CORE_SOURCE_DIR}/knowhere)
include_directories(${CORE_SOURCE_DIR})
include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda/lib64)

message(STATUS "arrow prefix: ${ARROW_PREFIX}")
message(STATUS "libjemalloc_pic path: ${ARROW_PREFIX}/lib/libjemalloc_pic.a")

set(depend_libs
        gtest gmock gtest_main gmock_main
        faiss openblas lapack
        arrow "${ARROW_PREFIX}/lib/libjemalloc_pic.a"
        )

set(basic_libs
        cudart cublas
        gomp gfortran pthread
        )

set(util_srcs
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.cc
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissGpuResourceMgr.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissIO.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/IndexParameter.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/adapter/Structure.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/adapter/ArrowAdapter.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/common/Exception.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/common/Timer.cpp
        ${CORE_SOURCE_DIR}/unittest/utils.cpp
        )

#<IVF-TEST>
set(ivf_srcs
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/Cloner.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVF.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVF.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQ.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVFSQ.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFPQ.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVFPQ.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQHybrid.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIDMAP.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/FaissBaseIndex.cpp
        )
if(NOT TARGET test_ivf)
    add_executable(test_ivf test_ivf.cpp ${ivf_srcs} ${util_srcs})
endif()
target_link_libraries(test_ivf ${depend_libs} ${unittest_libs} ${basic_libs})


#<IDMAP-TEST>
set(idmap_srcs
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIDMAP.cpp
        )
if(NOT TARGET test_idmap)
    add_executable(test_idmap test_idmap.cpp ${idmap_srcs} ${ivf_srcs} ${util_srcs})
endif()
target_link_libraries(test_idmap ${depend_libs} ${unittest_libs} ${basic_libs})

#<KDT-TEST>
set(kdt_srcs
        ${CORE_SOURCE_DIR}/knowhere/knowhere/adapter/SptagAdapter.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/preprocessor/Normalize.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/KDTParameterMgr.cpp
        ${CORE_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexKDT.cpp
        )
if(NOT TARGET test_kdt)
    add_executable(test_kdt test_kdt.cpp ${kdt_srcs} ${util_srcs})
endif()
target_link_libraries(test_kdt
        SPTAGLibStatic
        ${depend_libs} ${unittest_libs} ${basic_libs})

install(TARGETS test_ivf DESTINATION unittest)
install(TARGETS test_idmap DESTINATION unittest)
install(TARGETS test_kdt DESTINATION unittest)

#add_subdirectory(faiss_ori)
add_subdirectory(test_nsg)

