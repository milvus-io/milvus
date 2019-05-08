#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

aux_source_directory(cache cache_files)
aux_source_directory(config config_files)
aux_source_directory(server server_files)
aux_source_directory(utils utils_files)
aux_source_directory(db db_files)
aux_source_directory(wrapper wrapper_files)

set(license_check_files
        ${CMAKE_CURRENT_SOURCE_DIR}/license/License.cpp
        )

set(service_files
        thrift/gen-cpp/VecService.cpp
        thrift/gen-cpp/VectorService_constants.cpp
        thrift/gen-cpp/VectorService_types.cpp
        )

set(vecwise_engine_src
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        ${db_files}
        ${wrapper_files}
        )

include_directories(/usr/include)
include_directories(/usr/local/cuda/include)
find_library(cuda_library cudart cublas HINTS /usr/local/cuda/lib64)

cuda_add_library(vecwise_engine STATIC ${vecwise_engine_src})

set(engine_libs
        pthread
        libfaiss.a
        libgomp.a
        libopenblas.a
        libgfortran.a
        libquadmath.a
        cudart
        cublas
        libsqlite3.a
        )

target_link_libraries(vecwise_engine ${engine_libs} ${cuda_library})

add_executable(vecwise_server
        ${config_files}
        ${server_files}
        ${utils_files}
        ${service_files}
        ${license_check_files}
        ${VECWISE_THIRD_PARTY_BUILD}/include/easylogging++.cc
        )

set(server_libs
        vecwise_engine
        librocksdb.a
        libthrift.a
        pthread
        libyaml-cpp.a
        libboost_system.a
        libboost_filesystem.a
        pthread
        libsnappy.a
        libbz2.a
        libz.a
        libzstd.a
        liblz4.a
        )

target_link_libraries(vecwise_server ${server_libs})

set(license_generator_src
        ${CMAKE_CURRENT_SOURCE_DIR}/license/LicenseGenerator.cpp)

add_executable(license_generator ${license_generator_src})
