#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

aux_source_directory(cache cache_files)
aux_source_directory(config config_files)
aux_source_directory(server server_files)
aux_source_directory(utils utils_files)

set(vecwise_engine_src
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        )

add_library(vecwise_engine STATIC ${vecwise_engine_src})

add_executable(vecwise_engine_server
        ${config_files}
        ${server_files}
        ${utils_files}
        ${VECWISE_THIRD_PARTY_BUILD}/include/easylogging++.cc
        )

set(dependency_libs
        vecwise_engine
        yaml-cpp
        boost_system
        boost_filesystem
        )
target_link_libraries(vecwise_engine_server ${dependency_libs})