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
include_directories(${CMAKE_HOME_DIRECTORY}/src/thirdparty)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(
    ${KNOWHERE_INCLUDE_DIR}
    ${SIMDJSON_INCLUDE_DIR}
    ${TANTIVY_INCLUDE_DIR}
    ${CONAN_INCLUDE_DIRS}
    ${MILVUS_COMMON_INCLUDE_DIR}
    ${MILVUS_STORAGE_INCLUDE_DIR}
)

add_definitions(-DMILVUS_TEST_SEGCORE_YAML_PATH="${CMAKE_CURRENT_SOURCE_DIR}/test_utils/test_segcore.yaml")

# Collect test files from source directories using glob pattern
file(GLOB_RECURSE SOURCE_TEST_FILES
    "${CMAKE_HOME_DIRECTORY}/src/**/*Test.cpp"
    "${CMAKE_HOME_DIRECTORY}/src/**/*_test.cpp"
)

# TODO: better to use ls/find pattern
set(MILVUS_TEST_FILES
        ${SOURCE_TEST_FILES}
        init_gtest.cpp
        # test_packed_c.cpp
        # test_arrow_fs_c.cpp
        # test_column_groups_c.cpp
        # test_always_true_expr.cpp
        # test_array_bitmap_index.cpp
        # test_array_inverted_index.cpp
        # test_bf.cpp
        # test_bf_sparse.cpp
        # test_binary.cpp
        # test_binlog_index.cpp
        # test_bitmap_index.cpp
        # test_bool_index.cpp
        # test_c_api.cpp
        # test_chunk.cpp
        # test_chunk_vector.cpp
        # test_common.cpp
        # test_concurrent_vector.cpp
        # test_c_stream_reduce.cpp
        # test_c_tokenizer.cpp
        # test_loading.cpp
        # test_data_codec.cpp
        # test_delete_record.cpp
        # test_disk_file_manager_test.cpp
        # test_exec.cpp
        # test_expr.cpp
        # test_expr_materialized_view.cpp
        # test_file_writer.cpp
        # test_float16.cpp
        # test_function.cpp
        # test_futures.cpp
        # test_group_by.cpp
        # test_iterative_filter.cpp
        # test_growing.cpp
        # test_growing_index.cpp
        # test_hybrid_index.cpp
        # test_index_c_api.cpp
        # test_indexing.cpp
        # test_index_wrapper.cpp
        # test_init.cpp
        # test_integer_overflow.cpp
        # test_inverted_index.cpp
        # test_local_chunk_manager.cpp
        # test_mmap_chunk_manager.cpp
        # test_monitor.cpp
        # test_offset_ordered_array.cpp
        # test_offset_ordered_map.cpp
        # test_plan_proto.cpp
        # test_query.cpp
        # test_range_search_sort.cpp
        # test_reduce_c.cpp
        # test_reduce.cpp
        # test_regex_query.cpp
        # test_regex_query_util.cpp
        # test_relational.cpp
        # test_retrieve.cpp
        test_scalar_index.cpp
        # test_sealed.cpp
        # test_segcore.cpp
        # test_similarity_corelation.cpp
        # test_span.cpp
        # test_storage.cpp
        # test_string_expr.cpp
        # test_text_match.cpp
        # test_timestamp_index.cpp
        # test_tracer.cpp
        # test_utils.cpp
        # test_chunked_segment.cpp
        # test_chunked_column.cpp
        # test_rust_result.cpp
        # test_cached_search_iterator.cpp
        # test_random_sample.cpp
        # test_json_index.cpp
        # test_json_key_stats_index.cpp
        # test_types.cpp
        # test_growing_storage_v2.cpp
        # test_memory_planner.cpp
        # test_storage_v2_index_raw_data.cpp
        # test_chunked_column_group.cpp
        # test_group_chunk_translator.cpp
        # test_chunked_segment_storage_v2.cpp
        # test_thread_pool.cpp
        # test_json_flat_index.cpp
        # test_vector_array.cpp
        # test_ngram_query.cpp
        # test_stlsort_index.cpp
        test_string_index.cpp
        test_string_index_perf.cpp
        test_string_index_benchmark.cpp
        )

if ( NOT (INDEX_ENGINE STREQUAL "cardinal") )
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "KmeansClusteringTest\\.cpp$")
endif()

# need update aws-sdk-cpp, see more from https://github.com/aws/aws-sdk-cpp/issues/1757.
# now we always remove this file from MILVUS_TEST_FILES thus it is never compiled.
# once done, compile this test file only if `BUILD_DISK_ANN STREQUAL "ON"`.
# if ( BUILD_DISK_ANN STREQUAL "OFF" )
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "MinioChunkManagerTest\\.cpp$")
# endif()

# bitset has its own test binary
list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "BitsetTest\\.cpp$")

if (NOT (LINUX OR APPLE))
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "ScalarIndexCreatorTest\\.cpp$")
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "StringIndexTest\\.cpp$")
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "ArrayTest\\.cpp$")
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "ExprArrayTest\\.cpp$")
endif()

if (DEFINED AZURE_BUILD_DIR)
    include_directories("${AZURE_BUILD_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include")
else()
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "AzureChunkManagerTest\\.cpp$")
endif()
# need update aws-sdk-cpp, see more from https://github.com/aws/aws-sdk-cpp/issues/2119
# once done, move this line to the else branch of `if (DEFINED AZURE_BUILD_DIR)`
list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "RemoteChunkManagerTest\\.cpp$")

if (ENABLE_GCP_NATIVE)
    add_definitions(-DENABLE_GCP_NATIVE)
else()
    list(FILTER MILVUS_TEST_FILES EXCLUDE REGEX "GcpNativeChunkManagerTest\\.cpp$")
endif()

if (LINUX)
    message( STATUS "Building Milvus Unit Test on Linux")
    option(USE_ASAN "Whether to use AddressSanitizer" OFF)
    if ( USE_ASAN )
        message( STATUS "Building Milvus using AddressSanitizer")
        add_compile_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
        add_link_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
    endif()
endif()

add_executable(all_tests
        ${MILVUS_TEST_FILES}
        )

target_link_libraries(all_tests
        gtest
        gmock
        milvus_core
        knowhere
        milvus-storage
        milvus-common
        )

install(TARGETS all_tests DESTINATION unittest)

add_subdirectory(bench)
add_subdirectory(test_json_stats)

# bitset unit test
include(CheckCXXCompilerFlag)
include(CheckIncludeFileCXX)

check_cxx_compiler_flag("-march=armv8-a+sve" COMPILER_SUPPORTS_SVE)
check_include_file_cxx("arm_sve.h" COMPILER_HAS_ARM_SVE_HEADER)

add_executable(bitset_test
        ${CMAKE_HOME_DIRECTORY}/src/bitset/BitsetTest.cpp
)

if (COMPILER_SUPPORTS_SVE AND COMPILER_HAS_ARM_SVE_HEADER)
        message(STATUS "SVE support for the bitset library UT is enabled")
        target_compile_definitions(bitset_test PRIVATE BITSET_ENABLE_SVE_SUPPORT=1)

        set_source_files_properties(${CMAKE_HOME_DIRECTORY}/src/bitset/BitsetTest.cpp PROPERTIES COMPILE_FLAGS "-march=armv8-a+sve")
else()
        message(STATUS "SVE support for the bitset library UT is disabled")
endif()

target_link_libraries(bitset_test
        milvus_bitset
        gtest
        ${CONAN_LIBS}
)
install(TARGETS bitset_test DESTINATION unittest)
