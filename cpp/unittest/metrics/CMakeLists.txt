

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
#include_directories(../db/utils.h)
include_directories(../../src/metrics)


#set(metrics_src_files
#        ../../src/metrics/Metrics.cpp
#        ../../src/metrics/Metrics.h
#        ../../src/metrics/PrometheusMetrics.cpp
#        ../../src/metrics/MetricBase.h
#        ../../src/server/ServerConfig.cpp
#        ../../src/utils/CommonUtil.cpp
#        ../../src/utils/TimeRecorder.cpp
#        )

set(count_test_src
        ${unittest_srcs}
        ${config_files}
        ${cache_srcs}
        ${db_srcs}
        ${db_scheduler_srcs}
        ${wrapper_src}
        ${metrics_src}
        metrics_test.cpp
        ../db/utils.cpp
        )


add_executable(metrics_test ${count_test_src} ${require_files} )

target_link_libraries(metrics_test
        gpufaiss
        faiss
        cudart
        cublas
        sqlite3
        boost_system
        boost_filesystem
        lz4
        metrics
        gtest
#        prometheus-cpp-pull
#        prometheus-cpp-push
#        prometheus-cpp-core
        pthread
        z
        ${unittest_libs}
        )