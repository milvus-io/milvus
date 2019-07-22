

#set(CURL_LIBRARY "-lcurl")
#find_package(CURL REQUIRED)
#INCLUDE_DIRECTORIES(${CURL_INCLUDE_DIR})
#
#INCLUDE_DIRECTORIES(~/development/lib/usr/local/include)
#LINK_DIRECTORIES(~/development/lib/usr/local/lib)


include_directories(../../src)

aux_source_directory(../../src/db db_srcs)
aux_source_directory(../../src/config config_files)
aux_source_directory(../../src/cache cache_srcs)
aux_source_directory(../../src/wrapper wrapper_src)
aux_source_directory(../../src/metrics metrics_src)
aux_source_directory(./ test_srcs)

aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler scheduler_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/context scheduler_context_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/task scheduler_task_files)
set(db_scheduler_srcs
        ${scheduler_files}
        ${scheduler_context_files}
        ${scheduler_task_files}
        )

include_directories(/usr/include)
include_directories(../../third_party/build/include)
link_directories(../../third_party/build/lib)
include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")
include_directories(../../src/metrics)

include_directories(/usr/include/mysql)

set(count_test_src
        ${config_files}
        ${cache_srcs}
        ${db_srcs}
        ${db_scheduler_srcs}
        ${wrapper_src}
        ${metrics_src}
        ${test_srcs}
        )


add_executable(metrics_test ${count_test_src} ${require_files} )

target_link_libraries(metrics_test
        faiss
        cudart
        cublas
        sqlite
        boost_system_static
        boost_filesystem_static
        lz4
        metrics
        gtest
        pthread
        z
        mysqlpp
        ${unittest_libs}
        )
if(${BUILD_FAISS_WITH_MKL} STREQUAL "ON")
    target_link_libraries(metrics_test ${MKL_LIBS} ${MKL_LIBS})
else()
    target_link_libraries(metrics_test
            lapack
            openblas)
endif()

install(TARGETS metrics_test DESTINATION bin)