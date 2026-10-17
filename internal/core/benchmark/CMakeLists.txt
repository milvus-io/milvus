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
target_link_libraries(bit_sliced_index_benchmark PRIVATE
        milvus_core
        milvus_conan_deps
        knowhere
        milvus-storage)
install(TARGETS bit_sliced_index_benchmark DESTINATION benchmark)
