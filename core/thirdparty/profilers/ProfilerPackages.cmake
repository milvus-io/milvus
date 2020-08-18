if ( DEFINED ENV{MILVUS_LIBUNWIND_URL} )
    set( LIBUNWIND_SOURCE_URL "$ENV{MILVUS_LIBUNWIND_URL}" )
else ()
    set( LIBUNWIND_SOURCE_URL
         "https://github.com/libunwind/libunwind/releases/download/v${LIBUNWIND_VERSION}/libunwind-${LIBUNWIND_VERSION}.tar.gz" )
endif ()

if ( DEFINED ENV{MILVUS_GPERFTOOLS_URL} )
    set( GPERFTOOLS_SOURCE_URL "$ENV{MILVUS_GPERFTOOLS_URL}" )
else ()
    set( GPERFTOOLS_SOURCE_URL
         "https://github.com/gperftools/gperftools/releases/download/gperftools-${GPERFTOOLS_VERSION}/gperftools-${GPERFTOOLS_VERSION}.tar.gz" )
endif ()
# ----------------------------------------------------------------------
# libunwind

macro( build_libunwind )
    message( STATUS "Building libunwind-${LIBUNWIND_VERSION} from source" )

    set( LIBUNWIND_PREFIX           "${CMAKE_CURRENT_BINARY_DIR}/libunwind_ep-prefix/src/libunwind_ep/install" )
    set( LIBUNWIND_INCLUDE_DIR      "${LIBUNWIND_PREFIX}/include" )
    set( LIBUNWIND_SHARED_LIB       "${LIBUNWIND_PREFIX}/lib/libunwind${CMAKE_SHARED_LIBRARY_SUFFIX}" )
    set( LIBUNWIND_CONFIGURE_ARGS   "--prefix=${LIBUNWIND_PREFIX}" )

    ExternalProject_Add(
        libunwind_ep
            URL                 ${LIBUNWIND_SOURCE_URL}
            CONFIGURE_COMMAND   "./configure"   ${LIBUNWIND_CONFIGURE_ARGS}
            BUILD_COMMAND       ${MAKE}         ${MAKE_BUILD_ARGS}
            BUILD_IN_SOURCE     1
            INSTALL_COMMAND     ${MAKE}         install
            ${EP_LOG_OPTIONS}
            # BUILD_BYPRODUCTS
            # ${LIBUNWIND_SHARED_LIB}
            )

    file( MAKE_DIRECTORY "${LIBUNWIND_INCLUDE_DIR}" )

    add_library( libunwind SHARED IMPORTED )
    set_target_properties( libunwind
            PROPERTIES
                IMPORTED_LOCATION "${LIBUNWIND_SHARED_LIB}"
                INTERFACE_INCLUDE_DIRECTORIES "${LIBUNWIND_INCLUDE_DIR}" )

    add_dependencies( libunwind libunwind_ep )
endmacro()


# ----------------------------------------------------------------------
# gperftools

macro( build_gperftools )
    message( STATUS "Building gperftools-${GPERFTOOLS_VERSION} from source" )
    set( GPERFTOOLS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gperftools_ep-prefix/src/gperftools_ep/install" )
    set( GPERFTOOLS_INCLUDE_DIR "${GPERFTOOLS_PREFIX}/include" )
    set( GPERFTOOLS_STATIC_LIB "${GPERFTOOLS_PREFIX}/lib/libprofiler${CMAKE_STATIC_LIBRARY_SUFFIX}" )
    set( GPERFTOOLS_CONFIGURE_ARGS
            "--prefix=${GPERFTOOLS_PREFIX}"
            "--disable-tests"
            "--quiet"
            "cc=${CCACHE_FOUND} {CMAKE_C_COMPILER}"
            "cxx=${CCACHE_FOUND} {CMAKE_CXX_COMPILER}"
       )

    ExternalProject_Add(
        gperftools_ep
            URL                 ${GPERFTOOLS_SOURCE_URL}
            CONFIGURE_COMMAND   "./configure"   ${GPERFTOOLS_CONFIGURE_ARGS}
            BUILD_COMMAND       ${MAKE}         ${MAKE_BUILD_ARGS}
            INSTALL_COMMAND     ${MAKE}         install
            ${EP_LOG_OPTIONS}
            BUILD_IN_SOURCE
            1
            # BUILD_BYPRODUCTS
            # ${GPERFTOOLS_STATIC_LIB}
            )

    ExternalProject_Add_StepDependencies( gperftools_ep build libunwind_ep )

    file( MAKE_DIRECTORY "${GPERFTOOLS_INCLUDE_DIR}" )

    add_library( gperftools STATIC IMPORTED )
    set_target_properties( gperftools
            PROPERTIES
                IMPORTED_LOCATION               "${GPERFTOOLS_STATIC_LIB}"
                INTERFACE_INCLUDE_DIRECTORIES   "${GPERFTOOLS_INCLUDE_DIR}"
                INTERFACE_LINK_LIBRARIES        libunwind )
    add_dependencies(gperftools gperftools_ep)
    add_dependencies(gperftools libunwind_ep)
endmacro()


build_libunwind()
get_target_property( LIBUNWIND_INCLUDE_DIR libunwind INTERFACE_INCLUDE_DIRECTORIES )
include_directories( SYSTEM ${LIBUNWIND_INCLUDE_DIR} )
build_gperftools()
get_target_property(GPERFTOOLS_INCLUDE_DIR gperftools INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${GPERFTOOLS_INCLUDE_DIR})
link_directories(SYSTEM ${GPERFTOOLS_PREFIX}/lib)
