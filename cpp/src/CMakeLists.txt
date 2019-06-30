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
aux_source_directory(metrics metrics_files)

aux_source_directory(db/scheduler scheduler_files)
aux_source_directory(db/scheduler/context scheduler_context_files)
aux_source_directory(db/scheduler/task scheduler_task_files)
set(db_scheduler_files
        ${scheduler_files}
        ${scheduler_context_files}
        ${scheduler_task_files}
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
        thrift/gen-cpp/MilvusService.cpp
        thrift/gen-cpp/milvus_constants.cpp
        thrift/gen-cpp/milvus_types.cpp
        metrics/SystemInfo.cpp
        metrics/SystemInfo.h
        server/ThreadPoolServer.cpp
        server/ThreadPoolServer.h
        )

set(engine_files
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        ${db_files}
        ${db_scheduler_files}
        ${wrapper_files}
#        metrics/Metrics.cpp
        ${metrics_files}
        )

set(get_sys_info_files
        license/GetSysInfo.cpp)

set(s3_client_files
        storage/s3/S3ClientWrapper.cpp
        storage/s3/S3ClientWrapper.h)

include_directories(/usr/include)
include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
include_directories(thrift/gen-cpp)
include_directories(/usr/include/mysql)

set(third_party_libs
        easyloggingpp
        sqlite
        thrift
        yaml-cpp
        libgpufaiss.a
        faiss
        lapack
        openblas
        prometheus-cpp-push
        prometheus-cpp-pull
        prometheus-cpp-core
        boost_system_static
        boost_filesystem_static
        boost_serialization_static
        bzip2
        lz4
        snappy
        zlib
        zstd
        mysqlpp
        ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
        )
if (MEGASEARCH_WITH_ARROW STREQUAL "ON")
    set(third_party_libs ${third_party_libs} arrow)
endif() 

if (GPU_VERSION STREQUAL "ON")
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(engine_libs
            pthread
            libgomp.a
            libgfortran.a
            cudart
            cublas
            ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
            )
else()
    set(engine_libs
            pthread
            libgomp.a
            libgfortran.a
            ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
            )
endif ()


if (NOT ${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
    set(engine_libs
	    ${engine_libs}
	    libquadmath.a
	    )
endif ()

if (ENABLE_LICENSE STREQUAL "ON")
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs")
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(license_libs
            nvidia-ml
            crypto
            cudart
            cublas
            )
endif ()



cuda_add_library(milvus_engine STATIC ${engine_files})
target_link_libraries(milvus_engine ${engine_libs} ${third_party_libs})

add_library(metrics STATIC ${metrics_files})

if (ENABLE_LICENSE STREQUAL "ON")
        add_library(license_check STATIC ${license_check_files})
        target_link_libraries(license_check ${license_libs} ${third_party_libs})
endif ()

set(metrics_lib
        prometheus-cpp-push
        prometheus-cpp-pull
        prometheus-cpp-core
        )

target_link_libraries(metrics ${metrics_lib})

set(server_libs
        milvus_engine
        pthread
        dl
        metrics
        )

add_executable(milvus_server
        ${config_files}
        ${server_files}
        ${utils_files}
        ${service_files}
        ${metrics_files}
        )

if (ENABLE_LICENSE STREQUAL "ON")
        target_link_libraries(milvus_server ${server_libs} license_check ${third_party_libs})
else ()
        target_link_libraries(milvus_server ${server_libs} ${third_party_libs})
endif()

if (ENABLE_LICENSE STREQUAL "ON")
        add_executable(get_sys_info ${get_sys_info_files})
        add_executable(license_generator ${license_generator_files})

        target_link_libraries(get_sys_info ${license_libs} license_check ${third_party_libs})
        target_link_libraries(license_generator ${license_libs} ${third_party_libs})

        install(TARGETS get_sys_info DESTINATION bin)
        install(TARGETS license_generator DESTINATION bin)
endif ()

install(TARGETS milvus_server DESTINATION bin)

install(FILES
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3.2.4
        DESTINATION lib) #need to copy libmysqlpp.so

add_subdirectory(sdk)
