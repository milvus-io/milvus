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
    if(NOT TARGET gtest)
    	add_subdirectory( gtest )
    	add_subdirectory( google_benchmark)
    endif()
endif()


# ****************************** Thirdparty Arrow ***************************************
if(NOT TARGET boost_bitset_ext)
  add_subdirectory( boost_ext )
endif()
if(NOT TARGET fiu)
    add_subdirectory(fiu)
endif()
add_subdirectory( arrow )
#add_subdirectory( OpenBLAS )
add_subdirectory(NGT)
# 编译faiss
set(FAISS_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/faiss)
macro(build_mkl)
    if (FAISS_WITH_MKL)
        if (EXISTS "/proc/cpuinfo")
            FILE(READ /proc/cpuinfo PROC_CPUINFO)

            SET(VENDOR_ID_RX "vendor_id[ \t]*:[ \t]*([a-zA-Z]+)\n")
            STRING(REGEX MATCH "${VENDOR_ID_RX}" VENDOR_ID "${PROC_CPUINFO}")
            STRING(REGEX REPLACE "${VENDOR_ID_RX}" "\\1" VENDOR_ID "${VENDOR_ID}")

            if (NOT ${VENDOR_ID} STREQUAL "GenuineIntel")
                set(FAISS_WITH_MKL OFF)
            endif ()
        endif ()

        find_path(MKL_LIB_PATH
                NAMES "libmkl_intel_ilp64.a" "libmkl_gnu_thread.a" "libmkl_core.a"
                PATH_SUFFIXES "intel/compilers_and_libraries_${MKL_VERSION}/linux/mkl/lib/intel64/")
        if (${MKL_LIB_PATH} STREQUAL "MKL_LIB_PATH-NOTFOUND")
            message(FATAL_ERROR "Could not find MKL libraries")
        endif ()
        message(STATUS "MKL lib path = ${MKL_LIB_PATH}")

        set(MKL_LIBS
                ${MKL_LIB_PATH}/libmkl_intel_ilp64.a
                ${MKL_LIB_PATH}/libmkl_gnu_thread.a
                ${MKL_LIB_PATH}/libmkl_core.a
                )
    endif ()
endmacro()

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
    if (FAISS_WITH_MKL)
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "CPPFLAGS=-DFINTEGER=long -DMKL_ILP64 -m64 -I${MKL_LIB_PATH}/../../include"
                "LDFLAGS=-L${MKL_LIB_PATH}"
                )
    else ()
        message(STATUS "Build Faiss with OpenBlas/LAPACK")
        if(OpenBLAS_FOUND)
            set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "LDFLAGS=-L${OpenBLAS_LIB_DIR}")
        else()
            set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "LDFLAGS=-L${OPENBLAS_PREFIX}/lib")
        endif()
    endif ()

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

    if(NOT OpenBLAS_FOUND)
	message(STATUS "OpenBLAS FOUNDED")
        ExternalProject_Add_StepDependencies(faiss_ep configure openblas_ep)
    endif()

    file(MAKE_DIRECTORY "${FAISS_INCLUDE_DIR}")
    add_library(faiss STATIC IMPORTED)

    set_target_properties(
            faiss
            PROPERTIES
	    IMPORTED_GLOBAL TRUE
            IMPORTED_LOCATION "${FAISS_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${FAISS_INCLUDE_DIR}"
    )
    if (FAISS_WITH_MKL)
        set_target_properties(
                faiss
                PROPERTIES
                INTERFACE_LINK_LIBRARIES "${MKL_LIBS}")
    else ()
        set_target_properties(
                faiss
                PROPERTIES
                INTERFACE_LINK_LIBRARIES "${OpenBLAS_LIBRARIES}")
    endif ()

    add_dependencies(faiss faiss_ep)

endmacro()

if (KNOWHERE_WITH_FAISS AND NOT TARGET faiss_ep)

    if (FAISS_WITH_MKL)
        if (MKL_SOURCE STREQUAL "AUTO")
            find_package(MKL MODULE)
            if (NOT MKL_FOUND})
                build_mkl()
            endif ()
        elseif (MKL_SOURCE STREQUAL "BUNDLED")
            build_faiss()
        elseif (MKL_SOURCE STREQUAL "SYSTEM")
            find_package(FAISS_FOUND REQUIRED)
        endif ()  
    else ()
        message("faiss with no mkl")
    endif ()

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
