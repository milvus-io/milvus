# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Benchmarks are built with the unit tests but no test runs them. Compile them
# without coverage instrumentation so their sources are not reported as
# uncovered code; the linker flags still pull in the gcov runtime that the
# instrumented libraries need.
string(REPLACE "-fprofile-arcs" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-ftest-coverage" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

add_executable(fastmem_benchmark FastMemBenchmark.cpp)
target_include_directories(fastmem_benchmark PRIVATE ${CMAKE_HOME_DIRECTORY}/src)
target_link_libraries(fastmem_benchmark PRIVATE benchmark::benchmark)
install(TARGETS fastmem_benchmark DESTINATION benchmark)

# This component benchmark can also be built directly with a C++20 compiler;
# it uses the same membership helper as ScalarIndexSort without storage deps.
add_executable(sorted_int64_lookup_benchmark SortedInt64LookupBenchmark.cpp)
target_include_directories(sorted_int64_lookup_benchmark PRIVATE ${CMAKE_HOME_DIRECTORY}/src)
install(TARGETS sorted_int64_lookup_benchmark DESTINATION benchmark)

# Int32 bit-sliced index kernel (#53831): build time, size and query latency
# against the raw-column scan baseline. See src/index/scalar/bsi/README.md.
add_executable(bit_sliced_index_benchmark BitSlicedIndexBenchmark.cpp)
target_include_directories(bit_sliced_index_benchmark PRIVATE ${CMAKE_HOME_DIRECTORY}/src)
# Link milvus_core's runtime image directly, as index_tests does. Its legacy
# public link interface re-imports static archives already linked into it,
# and GNU ld then sees XXH64 from both libxxhash and milvus-storage's
# rust_bridge.
string(TOUPPER "${CMAKE_BUILD_TYPE}" BSI_BENCHMARK_BUILD_TYPE)
find_library(BSI_BENCHMARK_FOLLY_LIBRARY
        NAMES folly
        PATHS "${folly_Folly_folly_LIB_DIRS_${BSI_BENCHMARK_BUILD_TYPE}}"
        NO_DEFAULT_PATH
        NO_CACHE
        REQUIRED)
find_library(BSI_BENCHMARK_MILVUS_COMMON_LIBRARY
        NAMES milvus-common
        PATHS "${milvus-common_LIB_DIRS_${BSI_BENCHMARK_BUILD_TYPE}}"
        NO_DEFAULT_PATH
        NO_CACHE
        REQUIRED)
find_library(BSI_BENCHMARK_FMT_LIBRARY
        NAMES fmt
        PATHS "${fmt_fmt_fmt_LIB_DIRS_${BSI_BENCHMARK_BUILD_TYPE}}"
        NO_DEFAULT_PATH
        NO_CACHE
        REQUIRED)
target_link_libraries(bit_sliced_index_benchmark PRIVATE
        "$<COMPILE_ONLY:milvus_core>"
        "$<TARGET_FILE:milvus_core>"
        "$<TARGET_FILE:milvus-storage>"
        "$<TARGET_FILE:knowhere>"
        "${BSI_BENCHMARK_MILVUS_COMMON_LIBRARY}"
        gRPC::grpc++
        Boost::filesystem
        glog::glog
        "${BSI_BENCHMARK_FOLLY_LIBRARY}"
        "${BSI_BENCHMARK_FMT_LIBRARY}")
add_dependencies(bit_sliced_index_benchmark milvus_core)
install(TARGETS bit_sliced_index_benchmark DESTINATION benchmark)
