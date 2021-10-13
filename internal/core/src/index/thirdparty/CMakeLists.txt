add_compile_options( -O3 -fPIC -Wno-error -fopenmp )

if ( NOT KNOWHERE_VERBOSE_THIRDPARTY_BUILD )
    set( EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1 )
else ()
    set( EP_LOG_OPTIONS )
endif ()

set( MAKE_BUILD_ARGS "-j6" )

include( FetchContent )
set( FETCHCONTENT_BASE_DIR  ${INDEX_BINARY_DIR}/3rdparty_download )
set( FETCHCONTENT_QUIET     OFF )

if( CUSTOM_THIRDPARTY_DOWNLOAD_PATH )
    set( THIRDPARTY_DOWNLOAD_PATH ${CUSTOM_THIRDPARTY_DOWNLOAD_PATH} )
else()
    set( THIRDPARTY_DOWNLOAD_PATH ${CMAKE_BINARY_DIR}/3rdparty_download/download )
endif()
message( STATUS "Thirdparty downloaded file path: ${THIRDPARTY_DOWNLOAD_PATH}" )
# ----------------------------------------------------------------------
# Find pthreads

set( THREADS_PREFER_PTHREAD_FLAG ON )
find_package( Threads REQUIRED )


# ****************************** Thirdparty googletest ***************************************
if ( KNOWHERE_BUILD_TESTS )
    if(NOT TARGET gtest::gtest_main)
        add_subdirectory( gtest )
    endif()

    if(NOT TARGET benchmark::benchmark_main)
        add_subdirectory( google_benchmark )
    endif()
endif()


if(NOT TARGET fiu)
    add_subdirectory(fiu)
endif()

# ****************************** Thirdparty NGT ***************************************
add_subdirectory(NGT)

# ****************************** Thirdparty Faiss ***************************************
set(FAISS_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/faiss)

macro(build_faiss)
    message(STATUS "Building FAISS-${FAISS_VERSION} from source")

    set(FAISS_PREFIX "${INDEX_BINARY_DIR}/faiss_ep-prefix/src/faiss_ep")
    set(FAISS_INCLUDE_DIR "${FAISS_PREFIX}/include")
    set(FAISS_STATIC_LIB
            "${FAISS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}faiss${CMAKE_STATIC_LIBRARY_SUFFIX}")

    if (CCACHE_FOUND)
        set(FAISS_C_COMPILER "${CCACHE_FOUND} ${CMAKE_C_COMPILER}")
        if (MILVUS_GPU_VERSION)
            set(FAISS_CXX_COMPILER "${CMAKE_CXX_COMPILER}")
            set(FAISS_CUDA_COMPILER "${CCACHE_FOUND} ${CMAKE_CUDA_COMPILER}")
        else ()
            set(FAISS_CXX_COMPILER "${CCACHE_FOUND} ${CMAKE_CXX_COMPILER}")
        endif()
    else ()
        set(FAISS_C_COMPILER "${CMAKE_C_COMPILER}")
        set(FAISS_CXX_COMPILER "${CMAKE_CXX_COMPILER}")
    endif()

    set(FAISS_CONFIGURE_ARGS
            "--prefix=${FAISS_PREFIX}"
            "CC=${FAISS_C_COMPILER}"
            "CXX=${FAISS_CXX_COMPILER}"
            "NVCC=${FAISS_CUDA_COMPILER}"
            "CFLAGS=${EP_C_FLAGS}"
            "CXXFLAGS=${EP_CXX_FLAGS} -mf16c -O3"
            --without-python)

    message(STATUS "${FAISS_CONFIGURE_ARGS}")

    message(STATUS "Build Faiss with OpenBlas/LAPACK")
    set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
        "LDFLAGS=-L${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}"
        )

    if (MILVUS_GPU_VERSION)
        if (NOT MILVUS_CUDA_ARCH OR MILVUS_CUDA_ARCH STREQUAL "DEFAULT")
            set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "--with-cuda=${CUDA_TOOLKIT_ROOT_DIR}"
                "--with-cuda-arch=-gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61 -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_75,code=sm_75"
                )
        else()
            STRING(REPLACE ";" " " MILVUS_CUDA_ARCH "${MILVUS_CUDA_ARCH}")
            set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "--with-cuda=${CUDA_TOOLKIT_ROOT_DIR}"
                "--with-cuda-arch=${MILVUS_CUDA_ARCH}"
                )
        endif ()
    else ()
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "CPPFLAGS=-DUSE_CPU"
                --without-cuda)
    endif ()

    message(STATUS "Building FAISS with configure args -${FAISS_CONFIGURE_ARGS}")

    if (DEFINED ENV{FAISS_SOURCE_URL})
        set(FAISS_SOURCE_URL "$ENV{FAISS_SOURCE_URL}")
        externalproject_add(faiss_ep
                URL
                ${FAISS_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CONFIGURE_COMMAND
                "./configure"
                ${FAISS_CONFIGURE_ARGS}
                BUILD_COMMAND
                ${MAKE} ${MAKE_BUILD_ARGS} all
                BUILD_IN_SOURCE
                1
                INSTALL_COMMAND
                ${MAKE} install
                BUILD_BYPRODUCTS
                ${FAISS_STATIC_LIB})
    else ()
        externalproject_add(faiss_ep
                DOWNLOAD_COMMAND
                ""
                SOURCE_DIR
                ${FAISS_SOURCE_DIR}
                ${EP_LOG_OPTIONS}
                CONFIGURE_COMMAND
                "./configure"
                ${FAISS_CONFIGURE_ARGS}
                BUILD_COMMAND
                ${MAKE} ${MAKE_BUILD_ARGS} all
                BUILD_IN_SOURCE
                1
                INSTALL_COMMAND
                ${MAKE} install
                BUILD_BYPRODUCTS
                ${FAISS_STATIC_LIB})
    endif ()

    if(NOT BLAS_FOUND)
        message(STATUS "OpenBLAS BOUNDED")
        ExternalProject_Add_StepDependencies(faiss_ep configure openblas_ep)
    endif()

    file(MAKE_DIRECTORY "${FAISS_INCLUDE_DIR}")
    add_library(faiss STATIC IMPORTED)

    set_target_properties( faiss
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               "${FAISS_STATIC_LIB}"
                INTERFACE_INCLUDE_DIRECTORIES   "${FAISS_INCLUDE_DIR}")

    target_link_libraries( faiss INTERFACE openblas )
    add_dependencies(faiss faiss_ep)

endmacro()

if (KNOWHERE_WITH_FAISS AND NOT TARGET faiss_ep)
    if (FAISS_SOURCE STREQUAL "AUTO")
        find_package(FAISS MODULE)
        if (NOT FAISS_FOUND})
            build_faiss()
        endif ()
    elseif (FAISS_SOURCE STREQUAL "BUNDLED")
        build_faiss()
    elseif (FAISS_SOURCE STREQUAL "SYSTEM")
        find_package(FAISS REQUIRED)
    endif ()

    get_target_property(FAISS_INCLUDE_DIR faiss INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM "${FAISS_INCLUDE_DIR}")
    link_directories(SYSTEM ${FAISS_PREFIX}/lib/)
endif ()
