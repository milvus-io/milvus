

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


include_directories(/usr/include)
include_directories(../../third_party/build/include)
link_directories(../../third_party/build/lib)
include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")
#include_directories(../db/utils.h)
include_directories(../../src/metrics)


set(require_files
        ../../src/metrics/Metrics.cpp

#        ../../src/cache/CacheMgr.cpp
#        ../../src/metrics/PrometheusMetrics.cpp
        ../../src/server/ServerConfig.cpp
        ../../src/utils/CommonUtil.cpp
        ../../src/utils/TimeRecorder.cpp
#        ../../src/metrics/PrometheusMetrics.cpp
        )

set(count_test_src
        #        ${unittest_srcs}
        #        ${config_files}
        #        ${require_files}
        ${unittest_srcs}
        ${config_files}
        ${cache_srcs}
        ${db_srcs}
        ${wrapper_src}
        ${require_files}
        metrics_test.cpp
        ../db/utils.cpp
        ../../src/metrics/Metrics.h
        )


add_executable(metrics_test ${count_test_src} ${require_files} )

target_link_libraries(metrics_test

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