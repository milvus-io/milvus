
if (DEFINED ENV{MILVUS_AWS_URL})
    set(AWS_SOURCE_URL "$ENV{MILVUS_AWS_URL}")
else ()
    set(AWS_SOURCE_URL "https://github.com/aws/aws-sdk-cpp/archive/${AWS_VERSION}.tar.gz")
endif ()
# ----------------------------------------------------------------------
# aws

message(STATUS "Building aws-${AWS_VERSION} from source")
FetchContent_Declare(
        aws
        URL             ${AWS_SOURCE_URL}
        URL_MD5         "d7a3423ab2b7986290cdf63324080591"
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/aws-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/aws-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

set( ENABLE_TESTING     OFF CACHE BOOL "" FORCE )
set( BUILD_SHARED_LIBS  OFF CACHE BOOL "" FORCE )
set( FORCE_SHARED_CRT   OFF CACHE BOOL "" FORCE )
set( ENABLE_UNITY_BUILD OFF CACHE BOOL ""  FORCE )
# set( NO_ENCRYPTION      CACHE BOOL OFF FORCE )
set( BUILD_DEPS         OFF CACHE BOOL "" FORCE )
set( BUILD_ONLY         "s3" CACHE STRING "" FORCE )
FetchContent_GetProperties( aws )
if ( NOT aws_POPULATED )
    FetchContent_Populate( aws )


    # Adding the following targets:
    # AWS::aws-cpp-sdk-s3
    add_subdirectory( ${aws_SOURCE_DIR}
                      ${aws_BINARY_DIR}
                      EXCLUDE_FROM_ALL )
endif()

get_target_property( AWS_INCLUDE_DIR yaml-cpp INTERFACE_INCLUDE_DIRECTORIES )
message( STATUS ${AWS_INCLUDE_DIR} )

get_property( var DIRECTORY "${aws_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "aws compile options: ${var}" )

macro(build_aws)
    message(STATUS "Building aws-${AWS_VERSION} from source")
    set(AWS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/aws_ep-prefix/src/aws_ep")

    # set(AWS_CMAKE_ARGS
    #         ${EP_COMMON_TOOLCHAIN}
    #         "-DCMAKE_INSTALL_PREFIX=${AWS_PREFIX}"
    #         -DCMAKE_BUILD_TYPE=Release
    #         -DCMAKE_INSTALL_LIBDIR=lib
    #         -DBUILD_ONLY=s3
    #         -DBUILD_SHARED_LIBS=off
    #         -DENABLE_TESTING=off
    #         -DENABLE_UNITY_BUILD=on
    #         -DNO_ENCRYPTION=off)
    #
    # set(AWS_CPP_SDK_CORE_STATIC_LIB
    #         "${AWS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-core${CMAKE_STATIC_LIBRARY_SUFFIX}")
    # set(AWS_CPP_SDK_S3_STATIC_LIB
    #         "${AWS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-s3${CMAKE_STATIC_LIBRARY_SUFFIX}")
    # set(AWS_INCLUDE_DIR "${AWS_PREFIX}/include")
    # set(AWS_CMAKE_ARGS
    #         ${AWS_CMAKE_ARGS}
    #         -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    #         -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    #         -DCMAKE_C_FLAGS=${EP_C_FLAGS}
    #         -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS})

    # ExternalProject_Add(aws_ep
    #         ${EP_LOG_OPTIONS}
    #         CMAKE_ARGS
    #         ${AWS_CMAKE_ARGS}
    #         BUILD_COMMAND
    #         ${MAKE}
    #         ${MAKE_BUILD_ARGS}
    #         INSTALL_DIR
    #         ${AWS_PREFIX}
    #         URL
    #         ${AWS_SOURCE_URL}
    #         BUILD_BYPRODUCTS
    #         "${AWS_CPP_SDK_S3_STATIC_LIB}"
    #         "${AWS_CPP_SDK_CORE_STATIC_LIB}")
    #
    # file(MAKE_DIRECTORY "${AWS_INCLUDE_DIR}")
    # add_library(aws-cpp-sdk-s3 STATIC IMPORTED)
    # add_library(aws-cpp-sdk-core STATIC IMPORTED)
    #
    # set_target_properties(aws-cpp-sdk-s3
    #         PROPERTIES
    #         IMPORTED_LOCATION "${AWS_CPP_SDK_S3_STATIC_LIB}"
    #         INTERFACE_INCLUDE_DIRECTORIES "${AWS_INCLUDE_DIR}"
    #         )
    #
    # set_target_properties(aws-cpp-sdk-core
    #         PROPERTIES
    #         IMPORTED_LOCATION "${AWS_CPP_SDK_CORE_STATIC_LIB}"
    #         INTERFACE_INCLUDE_DIRECTORIES "${AWS_INCLUDE_DIR}"
    #         )
    #
    # if(REDHAT_FOUND)
    #     set_target_properties(aws-cpp-sdk-s3
    #             PROPERTIES
    #             INTERFACE_LINK_LIBRARIES
    #             "${AWS_PREFIX}/lib64/libaws-c-event-stream.a;${AWS_PREFIX}/lib64/libaws-checksums.a;${AWS_PREFIX}/lib64/libaws-c-common.a")
    #     set_target_properties(aws-cpp-sdk-core
    #             PROPERTIES
    #             INTERFACE_LINK_LIBRARIES
    #             "${AWS_PREFIX}/lib64/libaws-c-event-stream.a;${AWS_PREFIX}/lib64/libaws-checksums.a;${AWS_PREFIX}/lib64/libaws-c-common.a")
    # else()
    #     set_target_properties(aws-cpp-sdk-s3
    #             PROPERTIES
    #             INTERFACE_LINK_LIBRARIES
    #             "${AWS_PREFIX}/lib/libaws-c-event-stream.a;${AWS_PREFIX}/lib/libaws-checksums.a;${AWS_PREFIX}/lib/libaws-c-common.a")
    #     set_target_properties(aws-cpp-sdk-core
    #             PROPERTIES
    #             INTERFACE_LINK_LIBRARIES
    #             "${AWS_PREFIX}/lib/libaws-c-event-stream.a;${AWS_PREFIX}/lib/libaws-checksums.a;${AWS_PREFIX}/lib/libaws-c-common.a")
    # endif()
    #
    # add_dependencies(aws-cpp-sdk-s3 aws_ep)
    # add_dependencies(aws-cpp-sdk-core aws_ep)

endmacro()

# if(MILVUS_WITH_AWS)
#     resolve_dependency(AWS)
#
#     link_directories(SYSTEM ${AWS_PREFIX}/lib)
#
#     get_target_property(AWS_CPP_SDK_S3_INCLUDE_DIR aws-cpp-sdk-s3 INTERFACE_INCLUDE_DIRECTORIES)
#     include_directories(SYSTEM ${AWS_CPP_SDK_S3_INCLUDE_DIR})
#
#     get_target_property(AWS_CPP_SDK_CORE_INCLUDE_DIR aws-cpp-sdk-core INTERFACE_INCLUDE_DIRECTORIES)
#     include_directories(SYSTEM ${AWS_CPP_SDK_CORE_INCLUDE_DIR})
#
# endif()
