set(TBB_DIR ${CMAKE_SOURCE_DIR}/thirdparty/tbb)
set(TBB_LIBRARIES ${TBB_DIR}/libtbb.so)
include_directories(${TBB_DIR}/include)

include_directories(${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES})
link_directories(${CUDA_TOOLKIT_ROOT_DIR}/lib64)

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include)

set(SPTAG_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/SPTAG)
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

#add_library(SPTAGLib SHARED ${SRC_FILES} ${HDR_FILES})
#target_link_libraries(SPTAGLib ${TBB_LIBRARIES})
add_library(SPTAGLibStatic STATIC ${SRC_FILES} ${HDR_FILES})

set(external_srcs
        knowhere/adapter/sptag.cpp
        knowhere/adapter/structure.cpp
        knowhere/adapter/arrow.cpp
        knowhere/common/exception.cpp
        knowhere/common/timer.cpp
        )

set(index_srcs
        knowhere/index/preprocessor/normalize.cpp
        knowhere/index/vector_index/cpu_kdt_rng.cpp
        knowhere/index/vector_index/idmap.cpp
        knowhere/index/vector_index/ivf.cpp
        knowhere/index/vector_index/gpu_ivf.cpp
        knowhere/index/vector_index/kdt_parameters.cpp
        knowhere/index/vector_index/nsg_index.cpp
        knowhere/index/vector_index/nsg/nsg.cpp
        knowhere/index/vector_index/nsg/nsg_io.cpp
        knowhere/index/vector_index/nsg/utils.cpp
        knowhere/index/vector_index/cloner.cpp
        )

set(depend_libs
#        libtcmalloc.a
        SPTAGLibStatic
        ${TBB_LIBRARIES}
        faiss
        openblas
        lapack
        arrow
        jemalloc_pic
        cudart
        cublas
        gomp
        gfortran
        pthread
        )

add_library(
        knowhere STATIC
        ${external_srcs}
        ${index_srcs}
)
#target_compile_options(knowhere PUBLIC "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")

target_link_libraries(
        knowhere
        ${depend_libs}
)

#add_library(
#        knowhereS SHARED
#        ${external_srcs}
#        ${index_srcs}
#)
#
#target_link_libraries(
#        knowhereS
##        ${TBB_LIBRARIES}
#        ${depend_libs}
#)

INSTALL(TARGETS
        knowhere
        SPTAGLibStatic
        DESTINATION
        lib)

INSTALL(FILES
        ${ARROW_STATIC_LIB}
#        ${PARQUET_STATIC_LIB}
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

INSTALL(FILES ${CMAKE_SOURCE_DIR}/thirdparty/tbb/libtbb.so.2
#        RENAME "libtbb.so.2"
        DESTINATION lib
        )
INSTALL(FILES ${CMAKE_SOURCE_DIR}/thirdparty/tbb/libtbb.so
#        RENAME "libtbb.so"
        DESTINATION lib
        )

INSTALL(DIRECTORY
        ${CMAKE_SOURCE_DIR}/include/knowhere
        ${CMAKE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include/jsoncons
        ${CMAKE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include/jsoncons_ext
        ${ARROW_INCLUDE_DIR}/arrow
#        ${ARROW_INCLUDE_DIR}/parquet
        ${FAISS_PREFIX}/include/faiss
        ${OPENBLAS_INCLUDE_DIR}/
        ${CMAKE_SOURCE_DIR}/thirdparty/tbb/include/tbb
        DESTINATION
        include)

INSTALL(DIRECTORY
        ${SPTAG_SOURCE_DIR}/AnnService/inc/
        DESTINATION
        include/SPTAG/AnnService/inc)
