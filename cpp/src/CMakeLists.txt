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
#aux_source_directory(metrics metrics_files)

set(metrics_files
        metrics/Metrics.cpp
        metrics/MetricBase.h
)


set(license_check_files
        license/LicenseLibrary.cpp
        license/LicenseCheck.cpp
        )

set(license_generator_files
        license/LicenseGenerator.cpp
        license/LicenseLibrary.cpp
        )

set(service_files
        thrift/gen-cpp/MegasearchService.cpp
        thrift/gen-cpp/megasearch_constants.cpp
        thrift/gen-cpp/megasearch_types.cpp
        )

set(vecwise_engine_files
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        ${db_files}
        ${wrapper_files}
#        metrics/Metrics.cpp
        ${metrics_files}
        )

set(get_sys_info_files
        license/GetSysInfo.cpp)

include_directories(/usr/include)
include_directories(/usr/local/cuda/include)
include_directories(thrift/gen-cpp)

#target_link_libraries(megasearch boost_system_static)
#target_link_libraries(megasearch boost_filesystem_static)
#target_link_libraries(megasearch boost_serialization_static)
#target_link_libraries(megasearch bzip2)
#target_link_libraries(megasearch easyloggingpp)
#target_link_libraries(megasearch faiss)
#target_link_libraries(megasearch gtest)
#target_link_libraries(megasearch lapack)
#target_link_libraries(megasearch lz4)
#target_link_libraries(megasearch openblas)
#target_link_libraries(megasearch rocksdb)
#target_link_libraries(megasearch snappy)
#target_link_libraries(megasearch sqlite)
#target_link_libraries(megasearch sqlite_orm)
#target_link_libraries(megasearch thrift)
#target_link_libraries(megasearch yaml-cpp)
#target_link_libraries(megasearch zlib)
#target_link_libraries(megasearch zstd)

set(third_party_libs
        boost_system_static
        boost_filesystem_static
        boost_serialization_static
        bzip2
        easyloggingpp
        faiss
        gtest
        gtest_main
        gmock
        lapack
        lz4
        openblas
        prometheus-cpp-push
        prometheus-cpp-pull
        prometheus-cpp-core
        rocksdb
        snappy
        sqlite
#        sqlite_orm
        thrift
        yaml-cpp
        zlib
        zstd
        )

if (GPU_VERSION STREQUAL "ON")
    link_directories(/usr/local/cuda/lib64)
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
#            libprometheus-cpp-push.a
#            libprometheus-cpp-pull.a
#            libprometheus-cpp-core.a
            )
else()
    set(engine_libs
            pthread
            libfaiss.a
            libgomp.a
            libopenblas.a
            libgfortran.a
            libquadmath.a
            libsqlite3.a
#            libprometheus-cpp-push.a
#            libprometheus-cpp-pull.a
#            libprometheus-cpp-core.a
            )
endif ()

if (ENABLE_LICENSE STREQUAL "ON")
    link_directories(/usr/local/cuda/lib64/stubs)
    link_directories(/usr/local/cuda/lib64)
    set(license_libs
            nvidia-ml
            libboost_system.a
            libboost_filesystem.a
            libboost_serialization.a
            crypto
            cudart
            cublas
            )
endif ()



cuda_add_library(vecwise_engine STATIC ${vecwise_engine_files})
target_link_libraries(vecwise_engine ${engine_libs} ${third_party_libs})

add_library(metrics STATIC ${metrics_files})

if (ENABLE_LICENSE STREQUAL "ON")
        add_library(vecwise_license STATIC ${license_check_files})
        target_link_libraries(vecwise_license ${license_libs} ${third_party_libs})
endif ()

#set(metrics_lib
#        libprometheus-cpp-push.a
#        libprometheus-cpp-pull.a
#        libprometheus-cpp-core.a
#        )

#add_library(vecwise_engine STATIC ${metrics_files} )
#target_link_libraries(metrics ${metrics_lib})

set(server_libs
        vecwise_engine
        librocksdb.a
        libthrift.a
        pthread
        libyaml-cpp.a
        libboost_system.a
        libboost_filesystem.a
        libsnappy.a
        libbz2.a
        libz.a
        libzstd.a
        liblz4.a
        dl
        )

add_executable(vecwise_server
        ${config_files}
        ${server_files}
        ${utils_files}
        ${service_files}
        ${metrics_files}
        #${EASYLOGGINGPP_INCLUDE_DIR}/easylogging++.cc
        )

if (ENABLE_LICENSE STREQUAL "ON")
        target_link_libraries(vecwise_server ${server_libs} vecwise_license ${third_party_libs})
else ()
        target_link_libraries(vecwise_server ${server_libs} ${third_party_libs})
endif()

if (ENABLE_LICENSE STREQUAL "ON")
        add_executable(get_sys_info ${get_sys_info_files})
        add_executable(license_generator ${license_generator_files})

        target_link_libraries(get_sys_info ${license_libs} vecwise_license ${third_party_libs})
        target_link_libraries(license_generator ${license_libs} ${third_party_libs})

        install(TARGETS get_sys_info DESTINATION bin)
        install(TARGETS license_generator DESTINATION bin)
endif ()

install(TARGETS vecwise_server DESTINATION bin)

add_subdirectory(sdk)
#target_link_libraries(
#        libprometheus-cpp-push.a
#        libprometheus-cpp-pull.a
#        libprometheus-cpp-core.a
#        pthread
#        z
#        ${CURL_LIBRARIES})

