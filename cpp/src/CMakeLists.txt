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


set(license_check_files
        license/LicenseLibrary.cpp
        license/LicenseCheck.cpp
        )

set(license_generator_files
        license/LicenseGenerator.cpp
        license/LicenseLibrary.cpp
        ../unittest/metrics/counter_test.cpp ../unittest/metrics/metrics_test.cpp)

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
        metrics/Metrics.cpp
#        ${metrics_files}
        )

set(get_sys_info_files
        license/GetSysInfo.cpp)


include_directories(/usr/include)
include_directories(/usr/local/cuda/include)
include_directories(thrift/gen-cpp)

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
target_link_libraries(vecwise_engine ${engine_libs})

if (ENABLE_LICENSE STREQUAL "ON")
        add_library(vecwise_license STATIC ${license_check_files})
        target_link_libraries(vecwise_license ${license_libs})
endif ()

#set(metrics_lib
#        libprometheus-cpp-push.a
#        libprometheus-cpp-pull.a
#        libprometheus-cpp-core.a
#        )

#add_library(metrics STATIC ${metrics_files} metrics/MetricBase.h)
#target_link_libraries(metrics ${metrics_lib})

add_executable(vecwise_server
        ${config_files}
        ${server_files}
        ${utils_files}
        ${service_files}
#        ${metrics_files}
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
        libsnappy.a
        libbz2.a
        libz.a
        libzstd.a
        liblz4.a
        dl
        )

if (ENABLE_LICENSE STREQUAL "ON")
        target_link_libraries(vecwise_server ${server_libs} vecwise_license)
else ()
        target_link_libraries(vecwise_server ${server_libs})
endif()

if (ENABLE_LICENSE STREQUAL "ON")
        add_executable(license_generator ${license_generator_files})
        target_link_libraries(get_sys_info ${license_libs} vecwise_license)
        target_link_libraries(license_generator ${license_libs})
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

