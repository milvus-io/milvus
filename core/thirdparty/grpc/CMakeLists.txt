 # Another way is to use CMake's FetchContent module to clone gRPC at
# configure time. This makes gRPC's source code available to your project,
message(STATUS "Building GRPC-${GRPC_VERSION} from source")
# ******************************************************************
# FetchContent
# ******************************************************************
FetchContent_Declare(
    grpc
    URL     ${GRPC_SOURCE_URL}
    URL_MD5 "478215c151a144c2d8625b49ff1b70aa"
    SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/grpc-src
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/grpc-build
    )
set(FETCHCONTENT_QUIET OFF)

# Compile options for grpc only
add_compile_options(    ${EP_COMMON_TOOLCHAIN}
                        "-D CMAKE_BUILD_TYPE=Release"
                        "-D CMAKE_C_FLAGS=${EP_C_FLAGS}"
                        "-D CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}"
                        "-D CMAKE_CXX_FLAGS=${EP_CXX_FLAGS}"
                        "-D CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS}"
                        )

# using openSSL rather than boringssl
set(gRPC_SSL_PROVIDER "package" CACHE STRING "OPENSSL" FORCE)
# zlib build by grpc
set( gRPC_ZLIB_PROVIDER "module" )
FetchContent_GetProperties( grpc )
if ( NOT grpc_POPULATED )

    FetchContent_Populate( grpc
        )

    # Adding the following targets:
    # libprotobuf
    # grpc++_reflection
    # protoc (excutable)
    # grpc++
    add_subdirectory( ${grpc_SOURCE_DIR}
                      ${grpc_BINARY_DIR}
                      EXCLUDE_FROM_ALL
                      )
endif()

# get grpc COMPILE_OPTIONS )
get_property(var DIRECTORY "${grpc_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message(STATUS "grpc compile options: ${var}")


# **************************************************************************
# TODO grpc file dependee
# generate cc files
# **************************************************************************
set( PROTOC_EXCUTABLE           $<TARGET_FILE:protoc> )
set( GRPC_CPP_PLUGIN_EXCUTABLE  $<TARGET_FILE:grpc_cpp_plugin> )
set( PROTO_PATH          "${MILVUS_SOURCE_DIR}/src/grpc" )

# Proto file
get_filename_component( milvus_proto         "${PROTO_PATH}/milvus.proto"    ABSOLUTE )
get_filename_component( milvus_proto_path    "${PROTO_PATH}" PATH )

get_filename_component( status_proto         "${PROTO_PATH}/status.proto"    ABSOLUTE )
get_filename_component( status_proto_path    "${PROTO_PATH}"   PATH )


# Generated sources
set(milvus_proto_srcs "${PROTO_PATH}/gen-milvus-test/helloworld.pb.cc")
set(milvus_proto_hdrs "${PROTO_PATH}/gen-milvus-test/helloworld.pb.h")
set(milvus_grpc_srcs "${PROTO_PATH}/gen-milvus-test/helloworld.grpc.pb.cc")
set(milvus_grpc_hdrs "${PROTO_PATH}/gen-milvus-test/helloworld.grpc.pb.h")

set(status_proto_srcs "${PROTO_PATH}/gen-status-test/helloworld.pb.cc")
set(status_proto_hdrs "${PROTO_PATH}/gen-status-test/helloworld.pb.h")
set(status_grpc_srcs "${PROTO_PATH}/gen-status-test/helloworld.grpc.pb.cc")
set(status_grpc_hdrs "${PROTO_PATH}/gen-status-test/helloworld.grpc.pb.h")

add_custom_command(
      OUTPUT "${milvus_proto_srcs}" "${milvus_proto_hdrs}" "${milvus_grpc_srcs}" "${milvus_grpc_hdrs}"
      COMMAND ${PROTOC_EXCUTABLE}
      ARGS  --grpc_out  "${PROTO_PATH}/gen-milvus-test"
            --cpp_out   "${PROTO_PATH}/gen-milvus-test"
            -I          "${milvus_proto_path}"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXCUTABLE}"
            "${milvus_proto}"
      DEPENDS "${milvus_proto}" )

add_custom_command(
      OUTPUT "${status_proto_srcs}" "${status_proto_hdrs}" "${status_grpc_srcs}" "${status_grpc_hdrs}"
      COMMAND ${PROTOC_EXCUTABLE}
      ARGS  --grpc_out  "${PROTO_PATH}/gen-status-test"
            --cpp_out   "${PROTO_PATH}/gen-status-test"
            -I          "${status_proto_path}"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXCUTABLE}"
            "${status_proto}"
      DEPENDS "${status_proto}" )

# Include generated *.pb.h files
include_directories("${MILVUS_SOURCE_DIR}/gen-milvus-test")
include_directories("${MILVUS_SOURCE_DIR}/gen-status-test")


