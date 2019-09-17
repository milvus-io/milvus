#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(${MILVUS_ENGINE_SRC}/)
include_directories(/usr/include)

include_directories(/usr/include/mysql)

include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda/lib64)

aux_source_directory(${MILVUS_ENGINE_SRC}/db db_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/engine db_engine_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/insert db_insert_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta db_meta_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper/knowhere knowhere_src)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/action scheduler_action_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/event scheduler_event_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/resource scheduler_resource_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/task scheduler_task_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler scheduler_srcs)

aux_source_directory(${MILVUS_ENGINE_SRC}/server server_src)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl grpc_server_src)

aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler scheduler_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/context scheduler_context_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/task scheduler_task_files)

aux_source_directory(./ test_srcs)

set(db_scheduler_srcs
        ${scheduler_files}
        ${scheduler_context_files}
        ${scheduler_task_files}
        )

set(util_files
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.cc
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.h)

set(db_src
        ${config_files}
        ${cache_srcs}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${db_scheduler_srcs}
        ${wrapper_src}
        ${scheduler_action_srcs}
        ${scheduler_event_srcs}
        ${scheduler_resource_srcs}
        ${scheduler_task_srcs}
        ${scheduler_srcs}
        ${knowhere_src}
        ${util_files}
        ${require_files}
        ${test_srcs}
        )

set(utils_srcs
        ${MILVUS_ENGINE_SRC}/utils/StringHelpFunctions.cpp
        ${MILVUS_ENGINE_SRC}/utils/TimeRecorder.cpp
        ${MILVUS_ENGINE_SRC}/utils/CommonUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/LogUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/SignalUtil.cpp
        )

set(grpc_service_files
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
        )

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

cuda_add_executable(server_test
        ${db_src}
        ${unittest_srcs}
        ${grpc_server_src}
        ${server_src}
        ${utils_srcs}
        ${grpc_service_files}
        ${require_files}
        )

set(require_libs
        knowhere
        stdc++
        cudart
        cublas
        sqlite
        mysqlpp
        boost_system_static
        boost_filesystem_static
        snappy
        z
        bz2
        zstd
        lz4
        pthread
        grpcpp_channelz
        grpc++
        grpc
        grpc_protobuf
        grpc_protoc
        )

target_link_libraries(server_test
        ${require_libs}
        ${cuda_library}
        ${unittest_libs}
        )

install(TARGETS server_test DESTINATION unittest)

configure_file(appendix/server_config.yaml
        "${CMAKE_CURRENT_BINARY_DIR}/milvus/conf/server_config.yaml"
        COPYONLY)

configure_file(appendix/log_config.conf
        "${CMAKE_CURRENT_BINARY_DIR}/milvus/conf/log_config.conf"
        COPYONLY)
