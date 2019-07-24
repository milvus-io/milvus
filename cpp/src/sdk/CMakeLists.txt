#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

aux_source_directory(interface interface_files)

aux_source_directory(util util_files)

include_directories(/usr/include)
include_directories(include)
include_directories(/usr/local/include)

if (MILVUS_ENABLE_THRIFT STREQUAL "ON")
    aux_source_directory(thrift thrift_client_files)
    include_directories(thrift)
    include_directories(${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp)

    set(thrift_service_files
            ${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp/MilvusService.cpp
            ${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp/milvus_constants.cpp
            ${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp/milvus_types.cpp
            )

    add_library(milvus_thrift_sdk STATIC
            ${interface_files}
            ${thrift_client_files}
            ${util_files}
            ${thrift_service_files}
            )

    target_link_libraries(milvus_thrift_sdk
            ${third_party_libs}
            )
else()
    aux_source_directory(grpc grpc_client_files)

    include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus)
    include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-status)

    set(grpc_service_files
            ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.grpc.pb.cc
            ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.pb.cc
            ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.grpc.pb.cc
            ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.pb.cc
            )

    add_library(milvus_grpc_sdk STATIC
            ${interface_files}
            ${grpc_client_files}
            ${util_files}
            ${grpc_service_files}
            )

    target_link_libraries(milvus_grpc_sdk
            ${third_party_libs}
            )

endif()

if (MILVUS_ENABLE_THRIFT STREQUAL "ON")
    install(TARGETS milvus_thrift_sdk DESTINATION lib)
else()
    install(TARGETS milvus_grpc_sdk DESTINATION lib)
endif()

add_subdirectory(examples)
