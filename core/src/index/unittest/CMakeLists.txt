include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${INDEX_SOURCE_DIR}/knowhere)
include_directories(${INDEX_SOURCE_DIR})
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
        ${MILVUS_ENGINE_SRC}/external/easyloggingpp/easylogging++.cc
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissGpuResourceMgr.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissIO.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/IndexParameter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/adapter/Structure.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/adapter/ArrowAdapter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/Exception.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/Timer.cpp
        ${INDEX_SOURCE_DIR}/unittest/utils.cpp
        )

#<IVF-TEST>
set(ivf_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/Cloner.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVFSQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFPQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVFPQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQHybrid.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIDMAP.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/FaissBaseIndex.cpp
        )
if(NOT TARGET test_ivf)
    add_executable(test_ivf test_ivf.cpp ${ivf_srcs} ${util_srcs})
endif()
target_link_libraries(test_ivf ${depend_libs} ${unittest_libs} ${basic_libs})


#<IDMAP-TEST>
set(idmap_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIDMAP.cpp
        )
if(NOT TARGET test_idmap)
    add_executable(test_idmap test_idmap.cpp ${idmap_srcs} ${ivf_srcs} ${util_srcs})
endif()
target_link_libraries(test_idmap ${depend_libs} ${unittest_libs} ${basic_libs})

#<KDT-TEST>
set(kdt_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/adapter/SptagAdapter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/preprocessor/Normalize.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/KDTParameterMgr.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexKDT.cpp
        )
if(NOT TARGET test_kdt)
    add_executable(test_kdt test_kdt.cpp ${kdt_srcs} ${util_srcs})
endif()
target_link_libraries(test_kdt
        SPTAGLibStatic
        ${depend_libs} ${unittest_libs} ${basic_libs})

add_executable(test_gpuresource test_gpuresource.cpp ${util_srcs} ${ivf_srcs})
target_link_libraries(test_gpuresource ${depend_libs} ${unittest_libs} ${basic_libs})

add_executable(test_customized_index test_customized_index.cpp ${util_srcs} ${ivf_srcs})
target_link_libraries(test_customized_index ${depend_libs} ${unittest_libs} ${basic_libs})

install(TARGETS test_ivf DESTINATION unittest)
install(TARGETS test_idmap DESTINATION unittest)
install(TARGETS test_kdt DESTINATION unittest)
install(TARGETS test_gpuresource DESTINATION unittest)
install(TARGETS test_customized_index DESTINATION unittest)

#add_subdirectory(faiss_ori)
#add_subdirectory(faiss_benchmark)
add_subdirectory(test_nsg)

