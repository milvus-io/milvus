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

    # set( LIBUNWIND_PREFIX           "${CMAKE_CURRENT_BINARY_DIR}/libunwind_ep-prefix/src/libunwind_ep/install" )
    # set( LIBUNWIND_INCLUDE_DIR      "${LIBUNWIND_PREFIX}/include" )
    # set( LIBUNWIND_SHARED_LIB       "${LIBUNWIND_PREFIX}/lib/libunwind${CMAKE_SHARED_LIBRARY_SUFFIX}" )
    # set( LIBUNWIND_CONFIGURE_ARGS   "--prefix=${LIBUNWIND_PREFIX}" )

    set( LIBUNWIND_PREFIX  ${CMAKE_CURRENT_BINARY_DIR}/libunwind)
    ExternalProject_Add(
        libunwind_ep
            DOWNLOAD_DIR        ${CMAKE_BINARY_DIR}/3rdparty_download
            SOURCE_DIR          ${LIBUNWIND_PREFIX}/libunwind_src
            BINARY_DIR          ${LIBUNWIND_PREFIX}/libunwind_build
            INSTALL_DIR         ${LIBUNWIND_PREFIX}/libunwind_install
            URL                 ${LIBUNWIND_SOURCE_URL}
            URL_MD5             a04f69d66d8e16f8bf3ab72a69112cd6
            # CONFIGURE_COMMAND   "./configure"   ${LIBUNWIND_CONFIGURE_ARGS}
            CONFIGURE_COMMAND   <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
            BUILD_COMMAND       ${MAKE}         ${MAKE_BUILD_ARGS}
            # BUILD_IN_SOURCE     1
            INSTALL_COMMAND     ${MAKE}         install
            ${EP_LOG_OPTIONS}
            )

    # file( MAKE_DIRECTORY "${LIBUNWIND_INCLUDE_DIR}" )
    ExternalProject_Get_Property( libunwind_ep INSTALL_DIR )
    # message( FATAL_ERROR "install${INSTALL_DIR}" )

    add_library( libunwind SHARED IMPORTED )

    set_target_properties( libunwind
            PROPERTIES
                IMPORTED_GLOBAL TRUE
                IMPORTED_LOCATION "${INSTALL_DIR}/lib/libunwind.so"
                INTERFACE_INCLUDE_DIRECTORIES "${INSTALL_DIR}/include" )

    add_dependencies( libunwind libunwind_ep )
endmacro()


# ----------------------------------------------------------------------
# gperftools

macro( build_gperftools )
    message( STATUS "Building gperftools-${GPERFTOOLS_VERSION} from source" )
    # set( GPERFTOOLS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gperftools_ep-prefix/src/gperftools_ep/install" )
    set( GPERFTOOLS_INCLUDE_DIR "${GPERFTOOLS_PREFIX}/include" )
    set( GPERFTOOLS_STATIC_LIB "${GPERFTOOLS_PREFIX}/lib/libprofiler${CMAKE_STATIC_LIBRARY_SUFFIX}" )
    set( GPERFTOOLS_CONFIGURE_ARGS
            "--quiet"
            "cc=${CCACHE_FOUND} {CMAKE_C_COMPILER}"
            "cxx=${CCACHE_FOUND} {CMAKE_CXX_COMPILER}"
       )

   set( GPERFTOOLS_PREFIX  ${CMAKE_CURRENT_BINARY_DIR}/gperftools)
    ExternalProject_Add(
        gperftools_ep
            DEPENDS             libunwind_ep
            DOWNLOAD_DIR        ${CMAKE_BINARY_DIR}/3rdparty_download
            SOURCE_DIR          ${GPERFTOOLS_PREFIX}/gperftools_src
            BINARY_DIR          ${GPERFTOOLS_PREFIX}/gperftools_build
            INSTALL_DIR         ${GPERFTOOLS_PREFIX}/gperftools_install
            URL                 ${GPERFTOOLS_SOURCE_URL}
            URL_MD5             c6a852a817e9160c79bdb2d3101b4601
            CONFIGURE_COMMAND   <SOURCE_DIR>/configure
                                    --prefix=<INSTALL_DIR>
                                    ${GPERFTOOLS_CONFIGURE_ARGS}
            BUILD_COMMAND       ${MAKE}         ${MAKE_BUILD_ARGS}
            INSTALL_COMMAND     ${MAKE}         install
            ${EP_LOG_OPTIONS}
            # BUILD_IN_SOURCE     1
            )

    # ExternalProject_Add_StepDependencies( gperftools_ep build libunwind_ep )


    # file( MAKE_DIRECTORY "${GPERFTOOLS_INCLUDE_DIR}" )

    ExternalProject_Get_Property( gperftools_ep INSTALL_DIR )
    add_library( gperftools STATIC IMPORTED )
    set_target_properties( gperftools
            PROPERTIES
                IMPORTED_GLOBAL  TRUE
                IMPORTED_LOCATION               "${INSTALL_DIR}/lib/libprofiler.a"
                INTERFACE_INCLUDE_DIRECTORIES   "${INSTALL_DIR}/include"
                INTERFACE_LINK_LIBRARIES        libunwind
                )
    add_dependencies(gperftools gperftools_ep)
    # add_dependencies(gperftools libunwind_ep)
endmacro()


build_libunwind()
build_gperftools()
