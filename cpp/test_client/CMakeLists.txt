#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

include_directories(../src)

aux_source_directory(./src client_src)
aux_source_directory(../src/config config_files)

set(util_files
        ../src/utils/CommonUtil.cpp
        ../src/utils/LogUtil.cpp
        ../src/utils/TimeRecorder.cpp)

set(service_files
    ../src/thrift/gen-cpp/VecService.cpp
    ../src/thrift/gen-cpp/VectorService_constants.cpp
    ../src/thrift/gen-cpp/VectorService_types.cpp)

add_executable(test_client
    ./main.cpp
    ../src/server/ServerConfig.cpp
    ${client_src}
    ${service_files}
    ${config_files}
    ${util_files}
    ${VECWISE_THIRD_PARTY_BUILD}/include/easylogging++.cc)

link_directories(
    "${CMAKE_BINARY_DIR}/lib"
    "${VECWISE_THIRD_PARTY_BUILD}/lib"
)

set(client_libs
    yaml-cpp
    boost_system
    boost_filesystem
    thrift
    pthread)

target_link_libraries(test_client ${client_libs})

add_executable(skeleton_server
        ../src/thrift/gen-cpp/VecService_server.skeleton.cpp
        ../src/thrift/gen-cpp/VecService.cpp
        ../src/thrift/gen-cpp/VectorService_constants.cpp
        ../src/thrift/gen-cpp/VectorService_types.cpp)

target_link_libraries(skeleton_server thrift)
