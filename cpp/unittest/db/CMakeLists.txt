#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
aux_source_directory(${MILVUS_ENGINE_SRC}/db db_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper wrapper_src)
aux_source_directory(./ test_srcs)

aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler scheduler_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/context scheduler_context_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/task scheduler_task_files)
set(db_scheduler_srcs
        ${scheduler_files}
        ${scheduler_context_files}
        ${scheduler_task_files}
        )

include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

include_directories(/usr/include/mysql)

set(db_test_src
    #${unittest_srcs}
    ${config_files}
    ${cache_srcs}
    ${db_srcs}
    ${db_scheduler_srcs}
    ${wrapper_src}
    ${require_files}
    ${test_srcs})

cuda_add_executable(db_test ${db_test_src})

set(db_libs
        libgpufaiss.a
        faiss
        cudart
        cublas
        sqlite3
        boost_system
        boost_filesystem
        lz4
        mysqlpp
        )

target_link_libraries(db_test ${db_libs} ${unittest_libs})

install(TARGETS db_test DESTINATION bin)
