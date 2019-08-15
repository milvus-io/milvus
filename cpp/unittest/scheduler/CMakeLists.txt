#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
aux_source_directory(${MILVUS_ENGINE_SRC}/db db_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/engine db_engine_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/insert db_insert_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta db_meta_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper/knowhere knowhere_src)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler scheduler_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/context scheduler_context_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/task scheduler_task_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/resource scheduler_resource_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/task scheduler_task_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler scheduler_srcs)
aux_source_directory(./ test_srcs)

set(util_files ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp)

set(db_scheduler_srcs
        ${scheduler_files}
        ${scheduler_context_files}
        ${scheduler_task_files})

include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

include_directories(/usr/include/mysql)

#add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY)

set(scheduler_test_src
        ${unittest_srcs}
        ${test_srcs}
        ${scheduler_resource_srcs}
        ${scheduler_task_srcs}
        ${scheduler_srcs}

        ${config_files}
        ${cache_srcs}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${db_scheduler_srcs}
        ${wrapper_src}
        ${knowhere_src}
        ${util_files}
        ${require_files}
        )

cuda_add_executable(scheduler_test ${scheduler_test_src})

set(scheduler_libs
        sqlite
        boost_system_static
        boost_filesystem_static
        lz4
        mysqlpp
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
        cudart
        cublas
        )

if (${BUILD_FAISS_WITH_MKL} STREQUAL "ON")
    set(db_libs ${db_libs} ${MKL_LIBS} ${MKL_LIBS})
else ()
    set(db_libs ${db_libs}
            lapack
            openblas)
endif ()


target_link_libraries(scheduler_test ${scheduler_libs} ${knowhere_libs} ${unittest_libs})

install(TARGETS scheduler_test DESTINATION bin)

