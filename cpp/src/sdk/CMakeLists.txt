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
include_directories(${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp)

set(service_files
        ${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp/MegasearchService.cpp
        ${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp/megasearch_constants.cpp
        ${CMAKE_SOURCE_DIR}/src/thrift/gen-cpp/megasearch_types.cpp
        )

add_library(megasearch_sdk STATIC
        ${interface_files}
        ${client_files}
        ${util_files}
        ${service_files}
        )

link_directories(../../third_party/build/lib)
target_link_libraries(megasearch_sdk
        ${third_party_libs}
        )

add_subdirectory(examples)
