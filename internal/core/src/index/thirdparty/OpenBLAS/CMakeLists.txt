if (DEFINED ENV{KNOWHERE_OPENBLAS_URL})
    set(OPENBLAS_SOURCE_URL "$ENV{KNOWHERE_OPENBLAS_URL}")
else ()
    set(OPENBLAS_SOURCE_URL
            "https://github.com.cnpmjs.org/xianyi/OpenBLAS/archive/v${OPENBLAS_VERSION}.tar.gz")
endif ()

set(OPENBLAS_PREFIX "${INDEX_BINARY_DIR}/openblas_ep-prefix/src/openblas_ep")

macro(build_openblas)
    message(STATUS "Building OpenBLAS-${OPENBLAS_VERSION} from source")
    set(OpenBLAS_INCLUDE_DIR "${OPENBLAS_PREFIX}/include")
    set(OpenBLAS_LIB_DIR "${OPENBLAS_PREFIX}/lib")
    set(OPENBLAS_SHARED_LIB
            "${OPENBLAS_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(OPENBLAS_STATIC_LIB
            "${OPENBLAS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}openblas${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(OPENBLAS_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_BUILD_TYPE=Release
            -DBUILD_SHARED_LIBS=ON
            -DBUILD_STATIC_LIBS=ON
            -DTARGET=CORE2
            -DDYNAMIC_ARCH=1
            -DDYNAMIC_OLDER=1
            -DUSE_THREAD=0
            -DUSE_OPENMP=OPENBLAS_SOURCE_URL
            -DFC=gfortran
            -DCC=gcc
            -DINTERFACE64=0
            -DNUM_THREADS=128
            -DNO_LAPACKE=1
            "-DVERSION=${OPENBLAS_VERSION}"
            "-DCMAKE_INSTALL_PREFIX=${OPENBLAS_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib)

    externalproject_add(openblas_ep
	            URL
		    ${OPENBLAS_SOURCE_URL}
		    URL_MD5
                    "28cc19a6acbf636f5aab5f10b9a0dfe1" 
		    ${EP_LOG_OPTIONS}
		    CMAKE_ARGS
		    ${OPENBLAS_CMAKE_ARGS}
		    BUILD_COMMAND
		    ${MAKE}
		    ${MAKE_BUILD_ARGS}
		    BUILD_IN_SOURCE
		    1
		    INSTALL_COMMAND
		    ${MAKE}
		    PREFIX=${OPENBLAS_PREFIX}
		    install
		    BUILD_BYPRODUCTS
		    ${OPENBLAS_SHARED_LIB}
		    ${OPENBLAS_STATIC_LIB})

    file(MAKE_DIRECTORY "${OpenBLAS_INCLUDE_DIR}")
    add_library(openblas SHARED IMPORTED)

    set_target_properties(
            openblas
            PROPERTIES
            IMPORTED_LOCATION "${OPENBLAS_SHARED_LIB}"
            LIBRARY_OUTPUT_NAME "openblas"
            INTERFACE_INCLUDE_DIRECTORIES "${OpenBLAS_INCLUDE_DIR}")
    add_dependencies(openblas openblas_ep)
    get_target_property(OpenBLAS_INCLUDE_DIR openblas INTERFACE_INCLUDE_DIRECTORIES)
    set(OpenBLAS_LIBRARIES "${OPENBLAS_SHARED_LIB}")
endmacro()


if (KNOWHERE_WITH_OPENBLAS)
    if (OpenBLAS_SOURCE STREQUAL "AUTO")
        find_package(OpenBLAS MODULE)
        if (NOT ${OpenBLAS_FOUND})
            build_openblas()
        endif ()
    elseif (OpenBLAS_SOURCE STREQUAL "BUNDLED")
        build_openblas()
    elseif (OpenBLAS_SOURCE STREQUAL "SYSTEM")
        find_package(OpenBLAS REQUIRED)
    endif ()    
    include_directories(SYSTEM "${OpenBLAS_INCLUDE_DIR}")
    link_directories(SYSTEM "${OpenBLAS_LIB_DIR}")
endif()
