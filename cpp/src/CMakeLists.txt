#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

aux_source_directory(cache cache_files)
aux_source_directory(config config_files)
aux_source_directory(server server_files)
aux_source_directory(server/grpc_impl grpcserver_files)
aux_source_directory(server/thrift_impl thriftserver_files)
aux_source_directory(utils utils_files)
aux_source_directory(db db_main_files)
aux_source_directory(db/engine db_engine_files)
aux_source_directory(db/insert db_insert_files)
aux_source_directory(db/meta db_meta_files)
aux_source_directory(metrics metrics_files)
aux_source_directory(wrapper/knowhere knowhere_files)

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

if (MILVUS_WITH_THRIFT STREQUAL "ON")
set(thrift_service_files
        thrift/gen-cpp/MilvusService.cpp
        thrift/gen-cpp/milvus_constants.cpp
        thrift/gen-cpp/milvus_types.cpp
        metrics/SystemInfo.cpp
        metrics/SystemInfo.h
        server/thrift_impl/ThreadPoolServer.cpp
        server/thrift_impl/ThreadPoolServer.h
        )
else()
set(grpc_service_files
        metrics/SystemInfo.cpp
        metrics/SystemInfo.h
        grpc/gen-milvus/milvus.grpc.pb.cc
        grpc/gen-milvus/milvus.pb.cc
        grpc/gen-status/status.grpc.pb.cc
        grpc/gen-status/status.pb.cc
        )
endif()

set(db_files
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${db_scheduler_files}
        ${metrics_files}
        ${knowhere_files}
        )

set(s3_client_files
        storage/s3/S3ClientWrapper.cpp
        storage/s3/S3ClientWrapper.h)

include_directories(/usr/include)
include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
include_directories(thrift/gen-cpp)
include_directories(/usr/include/mysql)
include_directories(grpc/gen-status)
include_directories(grpc/gen-milvus)

if (MILVUS_WITH_THRIFT STREQUAL "ON")
    message("Build thrift server")
    add_definitions("-DMILVUS_ENABLE_THRIFT")
    set(client_thrift_lib
            thrift)
else()
    message("Build grpc server")
    set(client_grpc_lib
            grpcpp_channelz
            grpc++
            grpc
            grpc_protobuf
            grpc_protoc)
endif()

set(third_party_libs
        knowhere
        SPTAGLibStatic
        arrow
        jemalloc_pic
        faiss
        openblas
        lapack
        easyloggingpp
        sqlite
        ${client_thrift_lib}
        ${client_grpc_lib}
        yaml-cpp
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
        cudart
        cublas
        mysqlpp
        ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
        cudart
        )

if (MEGASEARCH_WITH_ARROW STREQUAL "ON")
    set(third_party_libs ${third_party_libs} arrow)
endif()
if(${BUILD_FAISS_WITH_MKL} STREQUAL "ON")
    set(third_party_libs ${third_party_libs}
            ${MKL_LIBS})
else()
    set(third_party_libs ${third_party_libs}
            lapack
            openblas)
endif()

if (MILVUS_ENABLE_PROFILING STREQUAL "ON")
    set(third_party_libs ${third_party_libs}
            gperftools
            libunwind)
endif()

if (GPU_VERSION STREQUAL "ON")
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(engine_libs
            pthread
            libgomp.a
            libgfortran.a
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


cuda_add_library(milvus_engine STATIC ${db_files})
target_link_libraries(milvus_engine ${engine_libs} ${third_party_libs})

add_library(metrics STATIC ${metrics_files})

set(metrics_lib
        easyloggingpp
        yaml-cpp
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

set(knowhere_libs
        knowhere
        SPTAGLibStatic
        arrow
        jemalloc_pic
        faiss
        openblas
        lapack
        tbb
        )

if (MILVUS_WITH_THRIFT STREQUAL "ON")
    add_executable(milvus_server
            ${config_files}
            ${server_files}
            ${thriftserver_files}
            ${utils_files}
            ${thrift_service_files}
            ${metrics_files}
            )
else()
    add_executable(milvus_server
            ${config_files}
            ${server_files}
            ${grpcserver_files}
            ${utils_files}
            ${grpc_service_files}
            ${metrics_files}
            )
endif()

if(MILVUS_WITH_THRIFT STREQUAL "ON")
    target_link_libraries(milvus_server ${server_libs} ${knowhere_libs} ${third_party_libs})
else()
    target_link_libraries(milvus_server ${server_libs} ${knowhere_libs} ${third_party_libs})
endif()


install(TARGETS milvus_server DESTINATION bin)

install(FILES
        ${KNOWHERE_BUILD_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}tbb${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${KNOWHERE_BUILD_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}tbb${CMAKE_SHARED_LIBRARY_SUFFIX}.2
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3.2.4
        DESTINATION lib)

add_subdirectory(sdk)
