# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(${CMAKE_HOME_DIRECTORY}/unittest)

set(bench_srcs 
    bench_naive.cpp
    bench_search.cpp
)

set(indexbuilder_bench_srcs
        bench_indexbuilder.cpp
)

add_executable(all_bench ${bench_srcs})
target_link_libraries(all_bench
        milvus_segcore
        log
        pthread
        )

target_link_libraries(all_bench benchmark::benchmark_main)

add_executable(indexbuilder_bench ${indexbuilder_bench_srcs})
target_link_libraries(indexbuilder_bench
        milvus_segcore
        milvus_indexbuilder
        log
        pthread
        knowhere
        milvus_utils
        )

target_link_libraries(indexbuilder_bench benchmark::benchmark_main)
