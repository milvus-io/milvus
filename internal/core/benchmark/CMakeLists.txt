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

# Expression scan benchmark; compare two runs with compare_expr_bench.py.
set(EXPR_BENCH_PLANPARSER_LIB_DIR ${CMAKE_HOME_DIRECTORY}/output/lib)

add_executable(expr_scan_benchmark ExprScanBenchmark.cpp)
target_include_directories(expr_scan_benchmark PRIVATE
    ${CMAKE_HOME_DIRECTORY}/src
    ${CMAKE_HOME_DIRECTORY}/src/thirdparty
    ${CMAKE_HOME_DIRECTORY}/unittest
    ${KNOWHERE_INCLUDE_DIR}
    ${SIMDJSON_INCLUDE_DIR}
    ${TANTIVY_INCLUDE_DIR}
    ${MILVUS_STORAGE_INCLUDE_DIR}
    ${CMAKE_HOME_DIRECTORY}/output/include
)
target_link_options(expr_scan_benchmark PRIVATE "-L${EXPR_BENCH_PLANPARSER_LIB_DIR}")
if (LINUX)
    target_link_options(expr_scan_benchmark PRIVATE "LINKER:--allow-multiple-definition")
endif()
target_link_libraries(expr_scan_benchmark PRIVATE
    benchmark::benchmark
    GTest::gtest
    milvus_core
    milvus_conan_deps
    knowhere
    milvus-storage
    milvus-planparser-cpp
)
set_target_properties(expr_scan_benchmark PROPERTIES
    BUILD_RPATH "${EXPR_BENCH_PLANPARSER_LIB_DIR}"
    INSTALL_RPATH "${EXPR_BENCH_PLANPARSER_LIB_DIR}"
)
install(TARGETS expr_scan_benchmark DESTINATION benchmark)
