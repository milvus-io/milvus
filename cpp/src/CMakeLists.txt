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
#aux_source_directory(wrapper wrapper_files)
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
        thrift
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


cuda_add_library(milvus_engine STATIC ${engine_files})
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

add_executable(milvus_server
        ${config_files}
        ${server_files}
        ${utils_files}
        ${service_files}
        ${metrics_files}
        )


target_link_libraries(milvus_server
        ${server_libs}
        ${third_party_libs}
        ${knowhere_libs}
        )

install(TARGETS milvus_server DESTINATION bin)

install(FILES
        ${KNOWHERE_BUILD_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}tbb${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${KNOWHERE_BUILD_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}tbb${CMAKE_SHARED_LIBRARY_SUFFIX}.2
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3.2.4
        DESTINATION lib)

add_subdirectory(sdk)
