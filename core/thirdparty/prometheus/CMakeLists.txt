if ( DEFINED ENV{MILVUS_PROMETHEUS_URL} )
    set( PROMETHEUS_SOURCE_URL "$ENV{PROMETHEUS_OPENBLAS_URL}" )
else ()
    set( PROMETHEUS_SOURCE_URL
         "https://github.com/milvus-io/prometheus-cpp/archive/${PROMETHEUS_VERSION}.zip" )
endif ()

# ----------------------------------------------------------------------
# Prometheus

message( STATUS "Building Prometheus-${PROMETHEUS_VERSION} from source" )

FetchContent_Declare(
        prometheus
        URL             ${PROMETHEUS_SOURCE_URL}
        URL_MD5         "6550819ae4d61c480a55a69f08159413"
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/prometheus-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/prometheus-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

set( ENABLE_TESTING OFF CACHE BOOL "" FORCE )
FetchContent_GetProperties( prometheus )
if ( NOT prometheus_POPULATED )
    FetchContent_Populate( prometheus )

    # Adding the following targets:
    # prometheus-cpp::core
    # prometheus-cpp::pull
    # prometheus-cpp::push
    add_subdirectory( ${prometheus_SOURCE_DIR}
                      ${prometheus_BINARY_DIR}
                      EXCLUDE_FROM_ALL )
endif()

# get prometheus COMPILE_OPTIONS
get_property( var DIRECTORY "${prometheus_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "prometheus src compile options: ${var}" )
