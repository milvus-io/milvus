#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

aux_source_directory(src/interface interface_files)
aux_source_directory(src/client client_files)
aux_source_directory(src/util util_files)

include_directories(src)
include_directories(include)
include_directories(/usr/include)
include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus)
include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-status)
include_directories(/usr/local/include)

set(service_files
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.pb.cc
        )

add_library(milvus_grpc_sdk STATIC
        ${interface_files}
        ${client_files}
        ${util_files}
        ${service_files}
        )

target_link_libraries(milvus_grpc_sdk
        ${third_party_libs}
        )

add_subdirectory(examples)

install(TARGETS milvus_grpc_sdk DESTINATION lib)
