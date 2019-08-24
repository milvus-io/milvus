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


aux_source_directory(grpc grpc_client_files)

include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus)
include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-status)

set(grpc_service_files
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.pb.cc
        )

add_library(milvus_sdk STATIC
        ${interface_files}
        ${grpc_client_files}
        ${util_files}
        ${grpc_service_files}
        )

target_link_libraries(milvus_sdk
        ${third_party_libs}
        )

install(TARGETS milvus_sdk DESTINATION lib)

add_subdirectory(examples)
