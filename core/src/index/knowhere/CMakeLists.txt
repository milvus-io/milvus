include_directories(${INDEX_SOURCE_DIR}/knowhere)
include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/SPTAG/AnnService)

set(SPTAG_SOURCE_DIR ${INDEX_SOURCE_DIR}/thirdparty/SPTAG)
file(GLOB HDR_FILES
        ${SPTAG_SOURCE_DIR}/AnnService/inc/Core/*.h
        ${SPTAG_SOURCE_DIR}/AnnService/inc/Core/Common/*.h
        ${SPTAG_SOURCE_DIR}/AnnService/inc/Core/BKT/*.h
        ${SPTAG_SOURCE_DIR}/AnnService/inc/Core/KDT/*.h
        ${SPTAG_SOURCE_DIR}/AnnService/inc/Helper/*.h)
file(GLOB SRC_FILES
        ${SPTAG_SOURCE_DIR}/AnnService/src/Core/*.cpp
        ${SPTAG_SOURCE_DIR}/AnnService/src/Core/Common/*.cpp
        ${SPTAG_SOURCE_DIR}/AnnService/src/Core/BKT/*.cpp
        ${SPTAG_SOURCE_DIR}/AnnService/src/Core/KDT/*.cpp
        ${SPTAG_SOURCE_DIR}/AnnService/src/Helper/*.cpp)

if (NOT TARGET SPTAGLibStatic)
    add_library(SPTAGLibStatic STATIC ${SRC_FILES} ${HDR_FILES})
endif ()

set(external_srcs
        knowhere/common/Exception.cpp
        knowhere/common/Log.cpp
        knowhere/common/Timer.cpp
        )

set(index_srcs
        knowhere/index/vector_index/adapter/SptagAdapter.cpp
        knowhere/index/vector_index/adapter/VectorAdapter.cpp
        knowhere/index/vector_index/helpers/FaissIO.cpp
        knowhere/index/vector_index/helpers/IndexParameter.cpp
        knowhere/index/vector_index/helpers/SPTAGParameterMgr.cpp
        knowhere/index/vector_index/impl/nsg/Distance.cpp
        knowhere/index/vector_index/impl/nsg/NSG.cpp
        knowhere/index/vector_index/impl/nsg/NSGHelper.cpp
        knowhere/index/vector_index/impl/nsg/NSGIO.cpp
        knowhere/index/vector_index/ConfAdapter.cpp
        knowhere/index/vector_index/ConfAdapterMgr.cpp
        knowhere/index/vector_index/FaissBaseBinaryIndex.cpp
        knowhere/index/vector_index/FaissBaseIndex.cpp
        knowhere/index/vector_index/IndexBinaryIDMAP.cpp
        knowhere/index/vector_index/IndexBinaryIVF.cpp
        knowhere/index/vector_index/IndexHNSW.cpp
        knowhere/index/vector_index/IndexIDMAP.cpp
        knowhere/index/vector_index/IndexIVF.cpp
        knowhere/index/vector_index/IndexIVFPQ.cpp
        knowhere/index/vector_index/IndexIVFSQ.cpp
        knowhere/index/vector_index/IndexNSG.cpp
        knowhere/index/vector_index/IndexSPTAG.cpp
        knowhere/index/vector_index/IndexType.cpp
        knowhere/index/vector_index/VecIndexFactory.cpp
        knowhere/index/vector_index/IndexAnnoy.cpp
        )

set(depend_libs
        SPTAGLibStatic
        faiss
        gomp
        gfortran
        pthread
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

if (KNOWHERE_GPU_VERSION)
    include_directories(${CUDA_INCLUDE_DIRS})
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(cuda_lib
            cudart
            cublas
            )
    set(depend_libs ${depend_libs}
            ${cuda_lib}
            )

    set(index_srcs ${index_srcs}
            knowhere/index/vector_index/gpu/IndexGPUIDMAP.cpp
            knowhere/index/vector_index/gpu/IndexGPUIVF.cpp
            knowhere/index/vector_index/gpu/IndexGPUIVFPQ.cpp
            knowhere/index/vector_index/gpu/IndexGPUIVFSQ.cpp
            knowhere/index/vector_index/gpu/IndexIVFSQHybrid.cpp
            knowhere/index/vector_index/helpers/Cloner.cpp
            knowhere/index/vector_index/helpers/FaissGpuResourceMgr.cpp
            )

endif ()

if (NOT TARGET knowhere)
    add_library(
            knowhere STATIC
            ${external_srcs}
            ${index_srcs}
    )
endif ()

target_link_libraries(
        knowhere
        ${depend_libs}
)

set(INDEX_INCLUDE_DIRS
        ${INDEX_SOURCE_DIR}/knowhere
        ${INDEX_SOURCE_DIR}/thirdparty
        ${INDEX_SOURCE_DIR}/thirdparty/SPTAG/AnnService
#        ${ARROW_INCLUDE_DIR}
        ${FAISS_INCLUDE_DIR}
        ${OPENBLAS_INCLUDE_DIR}
        ${LAPACK_INCLUDE_DIR}
        )

set(INDEX_INCLUDE_DIRS ${INDEX_INCLUDE_DIRS} PARENT_SCOPE)
