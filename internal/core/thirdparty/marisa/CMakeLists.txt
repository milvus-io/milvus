set( MARISA_VERSION "v0.2.6")
set( MARISA_MD5 "695cecf504ced27ac13aa33d97d69dd0")

if ( DEFINED ENV{MILVUS_MARISA_URL} )
    set( MARISA_SOURCE_URL "$ENV{MILVUS_MARISA_URL}" )
else ()
    set( MARISA_SOURCE_URL
            "https://github.com/s-yata/marisa-trie/archive/refs/tags/${MARISA_VERSION}.tar.gz" )
endif ()

macro(build_marisa)
    message(STATUS "Building marisa-${MARISA_VERSION} from source")

    set (MARISA_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
    set (MARISA_DIR "${CMAKE_CURRENT_BINARY_DIR}/src")
    if ( LINUX )
          set (MARISA_CONFIGURE_COMMAND cd ${MARISA_DIR} && libtoolize && autoreconf -i && ./configure --prefix=${MARISA_INSTALL_PREFIX})
    else()
          set (MARISA_CONFIGURE_COMMAND brew install automake && cd ${MARISA_DIR} && glibtoolize && autoreconf -i && ./configure --prefix=${MARISA_INSTALL_PREFIX})
    endif()

    set (MARISA_BUILD_COMMAND make -j)
    set (MARISA_INSTALL_COMMAND make install)

    message(${MARISA_DIR})

    externalproject_add(marisa_ep
            URL                 ${MARISA_SOURCE_URL}
            URL_MD5             ${MARISA_MD5}
            SOURCE_DIR          ${MARISA_DIR}
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND   ${MARISA_CONFIGURE_COMMAND}
            BUILD_COMMAND       ${MARISA_BUILD_COMMAND}
            INSTALL_COMMAND     ${MARISA_INSTALL_COMMAND}
            )

    if( NOT IS_DIRECTORY ${MARISA_INSTALL_PREFIX}/include )
        file( MAKE_DIRECTORY "${MARISA_INSTALL_PREFIX}/include" )
    endif()

    add_library(marisa SHARED IMPORTED)
    set_target_properties( marisa
            PROPERTIES
            IMPORTED_GLOBAL     TRUE
            IMPORTED_LOCATION   ${MARISA_INSTALL_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}marisa${CMAKE_SHARED_LIBRARY_SUFFIX}
            INTERFACE_INCLUDE_DIRECTORIES ${MARISA_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR})

    get_target_property(MARISA_IMPORTED_LOCATION marisa IMPORTED_LOCATION)
    get_target_property(MARISA_INTERFACE_INCLUDE_DIRECTORIES marisa INTERFACE_INCLUDE_DIRECTORIES)
    message("MARISA_INSTALL_PREFIX: ${MARISA_INSTALL_PREFIX}")
    message("CMAKE_INSTALL_LIBDIR: ${CMAKE_INSTALL_LIBDIR}")
    message("MARISA_IMPORTED_LOCATION: ${MARISA_IMPORTED_LOCATION}")
    message("MARISA_INTERFACE_INCLUDE_DIRECTORIES: ${MARISA_INTERFACE_INCLUDE_DIRECTORIES}")

    add_dependencies(marisa marisa_ep)
endmacro()

set(MARISA_SOURCE "AUTO")
if (MARISA_SOURCE STREQUAL "AUTO")
    find_package(marisa)
    message(STATUS "marisa libraries: ${MARISA_LIBRARIES}")
    message(STATUS "marisa found: ${MARISA_FOUND}")

    if (MARISA_FOUND)
        add_library(marisa)
    else()
        build_marisa()
    endif()
elseif (MARISA_SOURCE STREQUAL "BUNDLED")
    build_marisa()
elseif (MARISA_SOURCE STREQUAL "SYSTEM")
    find_package(marisa)
    add_library(marisa)
endif ()
