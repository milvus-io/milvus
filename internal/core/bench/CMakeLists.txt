include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(${CMAKE_HOME_DIRECTORY}/unittest)
include_directories(${CMAKE_HOME_DIRECTORY}/src/index/knowhere)

set(bench_srcs 
    bench_naive.cpp
    bench_search.cpp
)

add_executable(all_bench ${bench_srcs})
target_link_libraries(all_bench
        milvus_segcore
        milvus_indexbuilder
        log
        pthread
        )

target_link_libraries(all_bench benchmark::benchmark_main)
