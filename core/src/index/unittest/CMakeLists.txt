include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${INDEX_SOURCE_DIR}/knowhere)
include_directories(${INDEX_SOURCE_DIR})

set(depend_libs
        gtest gmock gtest_main gmock_main
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
            ${BLAS_LIBRARIES}
            ${LAPACK_LIBRARIES}
            )
endif ()

set(basic_libs
        gomp gfortran pthread
        )

set(util_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/common/easyloggingpp/easylogging++.cc
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/FaissIO.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/IndexParameter.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/adapter/VectorAdapter.cpp
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

#<IVF-TEST>
set(ivf_srcs
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVF.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFPQ.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIDMAP.cpp
        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/FaissBaseIndex.cpp
        )
if (KNOWHERE_GPU_VERSION)
    set(ivf_srcs ${ivf_srcs}
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIDMAP.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/Cloner.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVF.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVFSQ.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexGPUIVFPQ.cpp
            ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexIVFSQHybrid.cpp
            )
endif ()
if (NOT TARGET test_ivf)
    add_executable(test_ivf test_ivf.cpp ${ivf_srcs} ${util_srcs})
endif ()
target_link_libraries(test_ivf ${depend_libs} ${unittest_libs} ${basic_libs})


#<IDMAP-TEST>
if (NOT TARGET test_idmap)
    add_executable(test_idmap test_idmap.cpp ${ivf_srcs} ${util_srcs})
endif ()
target_link_libraries(test_idmap ${depend_libs} ${unittest_libs} ${basic_libs})

#<SPTAG-TEST>
#set(sptag_srcs
#        ${INDEX_SOURCE_DIR}/knowhere/knowhere/adapter/SptagAdapter.cpp
#        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/preprocessor/Normalize.cpp
#        ${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/helpers/SPTAGParameterMgr.cpp
#	${INDEX_SOURCE_DIR}/knowhere/knowhere/index/vector_index/IndexSPTAG.cpp
#        )
#if (NOT TARGET test_sptag)
#    add_executable(test_sptag test_sptag.cpp ${sptag_srcs} ${util_srcs})
#endif ()
#target_link_libraries(test_sptag
#        SPTAGLibStatic
#        ${depend_libs} ${unittest_libs} ${basic_libs})

if (KNOWHERE_GPU_VERSION)
    add_executable(test_gpuresource test_gpuresource.cpp ${util_srcs} ${ivf_srcs})
    target_link_libraries(test_gpuresource ${depend_libs} ${unittest_libs} ${basic_libs})

    add_executable(test_customized_index test_customized_index.cpp ${util_srcs} ${ivf_srcs})
    target_link_libraries(test_customized_index ${depend_libs} ${unittest_libs} ${basic_libs})
endif ()

install(TARGETS test_ivf DESTINATION unittest)
install(TARGETS test_idmap DESTINATION unittest)
#install(TARGETS test_sptag DESTINATION unittest)
if (KNOWHERE_GPU_VERSION)
    install(TARGETS test_gpuresource DESTINATION unittest)
    install(TARGETS test_customized_index DESTINATION unittest)
endif ()
#add_subdirectory(faiss_ori)
#add_subdirectory(faiss_benchmark)
add_subdirectory(test_nsg)

