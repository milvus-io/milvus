set(TBB_DIR ${CORE_SOURCE_DIR}/thirdparty/tbb)
set(TBB_LIBRARIES ${TBB_DIR}/libtbb.so)
include_directories(${TBB_DIR}/include)

include_directories(${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES})
link_directories(${CUDA_TOOLKIT_ROOT_DIR}/lib64)

include_directories(${CORE_SOURCE_DIR}/knowhere)
include_directories(${CORE_SOURCE_DIR}/thirdparty)
include_directories(${CORE_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${CORE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include)

set(SPTAG_SOURCE_DIR ${CORE_SOURCE_DIR}/thirdparty/SPTAG)
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

if(NOT TARGET SPTAGLibStatic)
    add_library(SPTAGLibStatic STATIC ${SRC_FILES} ${HDR_FILES})
endif()

set(external_srcs
        knowhere/adapter/SptagAdapter.cpp
        knowhere/adapter/Structure.cpp
        knowhere/adapter/ArrowAdapter.cpp
        knowhere/common/Exception.cpp
        knowhere/common/Timer.cpp
        )

set(index_srcs
        knowhere/index/preprocessor/Normalize.cpp
        knowhere/index/vector_index/IndexKDT.cpp
        knowhere/index/vector_index/IndexIDMAP.cpp
        knowhere/index/vector_index/IndexIVF.cpp
        knowhere/index/vector_index/IndexGPUIVF.cpp
        knowhere/index/vector_index/utils/KDTParameterMgr.cpp
        knowhere/index/vector_index/IndexNSG.cpp
        knowhere/index/vector_index/nsg/NSG.cpp
        knowhere/index/vector_index/nsg/NSGIO.cpp
        knowhere/index/vector_index/nsg/NSGHelper.cpp
        knowhere/index/vector_index/utils/Cloner.cpp
        knowhere/index/vector_index/utils/FaissGpuResourceMgr.cpp
        )

set(depend_libs
        SPTAGLibStatic
        ${TBB_LIBRARIES}
        faiss
        openblas
        lapack
        arrow
        ${ARROW_PREFIX}/lib/libjemalloc_pic.a
        cudart
        cublas
        gomp
        gfortran
        pthread
        )

if(NOT TARGET knowhere)
    add_library(
            knowhere STATIC
            ${external_srcs}
            ${index_srcs}
    )
endif()

target_link_libraries(
        knowhere
        ${depend_libs}
)

INSTALL(TARGETS
        knowhere
        SPTAGLibStatic
        DESTINATION
        lib)

INSTALL(FILES
        ${ARROW_STATIC_LIB}
        ${ARROW_PREFIX}/lib/libjemalloc_pic.a
        ${FAISS_STATIC_LIB}
        ${LAPACK_STATIC_LIB}
        ${BLAS_STATIC_LIB}
        DESTINATION
        lib
        )

INSTALL(FILES ${OPENBLAS_REAL_STATIC_LIB}
        RENAME "libopenblas.a"
        DESTINATION lib
        )

INSTALL(FILES ${CORE_SOURCE_DIR}/thirdparty/tbb/libtbb.so.2
        DESTINATION lib
        )
INSTALL(FILES ${CORE_SOURCE_DIR}/thirdparty/tbb/libtbb.so
        DESTINATION lib
        )

set(CORE_INCLUDE_DIRS
        ${CORE_SOURCE_DIR}/knowhere
        ${CORE_SOURCE_DIR}/thirdparty
        ${CORE_SOURCE_DIR}/thirdparty/SPTAG/AnnService
        ${CORE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include
        ${ARROW_INCLUDE_DIR}
        ${FAISS_INCLUDE_DIR}
        ${OPENBLAS_INCLUDE_DIR}
        ${LAPACK_INCLUDE_DIR}
        ${CORE_SOURCE_DIR}/thirdparty/tbb/include
        )

set(CORE_INCLUDE_DIRS ${CORE_INCLUDE_DIRS} PARENT_SCOPE)

#INSTALL(DIRECTORY
#        ${CORE_SOURCE_DIR}/include/knowhere
#        ${CORE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include/jsoncons
#        ${CORE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include/jsoncons_ext
#        ${ARROW_INCLUDE_DIR}/arrow
#        ${FAISS_PREFIX}/include/faiss
#        ${OPENBLAS_INCLUDE_DIR}/
#        ${CORE_SOURCE_DIR}/thirdparty/tbb/include/tbb
#        DESTINATION
#        include)
#
#INSTALL(DIRECTORY
#        ${SPTAG_SOURCE_DIR}/AnnService/inc/
#        DESTINATION
#        include/SPTAG/AnnService/inc)
