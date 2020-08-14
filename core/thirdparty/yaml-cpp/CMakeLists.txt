# ----------------------------------------------------------------------
# yaml-cpp

if (DEFINED ENV{MILVUS_YAMLCPP_URL})
    set(YAMLCPP_SOURCE_URL "$ENV{MILVUS_YAMLCPP_URL}")
else ()
    set(YAMLCPP_SOURCE_URL "https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-${YAMLCPP_VERSION}.tar.gz"

                           "https://gitee.com/quicksilver/yaml-cpp/repository/archive/yaml-cpp-${YAMLCPP_VERSION}.zip"
                           )
endif ()

message(STATUS "Building yaml-cpp-${YAMLCPP_VERSION} from source")
#*********************************FC
FetchContent_Declare(
    yaml-cpp
    URL         ${YAMLCPP_SOURCE_URL}
    URL_MD5     "b45bf1089a382e81f6b661062c10d0c2"
    SOURCE_DIR  ${CMAKE_CURRENT_BINARY_DIR}/yaml-src
    BINARY_DIR  ${CMAKE_CURRENT_BINARY_DIR}/yaml-build
    )

# Compile options for grpc only

set( YAML_CPP_BUILD_TESTS CACHE BOOL OFF FORCE )
set( YAML_CPP_BUILD_TOOLS CACHE BOOL OFF FORCE )
FetchContent_GetProperties( yaml-cpp )
if ( NOT yaml-cpp_POPULATED )

    FetchContent_Populate( yaml-cpp )

    # rm cmake_policy warning
    # execute_process( COMMAND cmake ${yaml-cpp_SOURCE_DIR} -Wno-dev WORKING_DIRECTORY ${yaml-cpp_BINARY_DIR} )

    # Adding the following targets:
    # yaml-cpp::yaml-cpp, yaml-cpp
    add_subdirectory( ${yaml-cpp_SOURCE_DIR}
                      ${yaml-cpp_BINARY_DIR}
                      EXCLUDE_FROM_ALL )

endif()

get_target_property( YAML_CPP_INCLUDE_DIR yaml-cpp INTERFACE_INCLUDE_DIRECTORIES )
message(STATUS ${YAML_CPP_INCLUDE_DIR})

# TODO yaml-cpp doesn't have an INTERFACE INCLUDE DIR properties
get_property(var DIRECTORY "${yaml-cpp_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message(STATUS "yaml compile options: ${var}")
