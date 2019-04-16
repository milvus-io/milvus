#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

aux_source_directory(cache cache_files)
aux_source_directory(config config_files)
aux_source_directory(server server_files)
aux_source_directory(utils utils_files)
aux_source_directory(wrapper wrapper_files)

set(service_files
        thrift/gen-cpp/VecService.cpp
        thrift/gen-cpp/VectorService_constants.cpp
        thrift/gen-cpp/VectorService_types.cpp)

set(vecwise_engine_src
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        ${wrapper_files})

include_directories(/usr/local/cuda/include)
find_library(cuda_library cudart cublas HINTS /usr/local/cuda/lib64)

add_library(vecwise_engine STATIC ${vecwise_engine_src})

add_executable(vecwise_engine_server
        ${config_files}
        ${server_files}
        ${utils_files}
<<<<<<< f0f0e4c6c70323464d2e3bb4834e942bdbe2c868
        ${wrapper_files}
=======
        ${service_files}
>>>>>>> add test client
        ${VECWISE_THIRD_PARTY_BUILD}/include/easylogging++.cc
        )

set(dependency_libs
        vecwise_engine
        yaml-cpp
        boost_system
        boost_filesystem
        thrift
        pthread
        )
target_link_libraries(vecwise_engine_server ${dependency_libs} ${cuda_library})