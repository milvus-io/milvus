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
include_directories(
    ${KNOWHERE_INCLUDE_DIR}
    ${SIMDJSON_INCLUDE_DIR}
    ${TANTIVY_INCLUDE_DIR}
    ${CONAN_INCLUDE_DIRS}
    ${MILVUS_STORAGE_INCLUDE_DIR}
)

add_definitions(-DMILVUS_TEST_SEGCORE_YAML_PATH="${CMAKE_SOURCE_DIR}/unittest/test_utils/test_segcore.yaml")

# TODO: better to use ls/find pattern
set(MILVUS_TEST_FILES
        init_gtest.cpp
        test_packed_c.cpp
        test_arrow_fs_c.cpp
        test_column_groups_c.cpp
        test_always_true_expr.cpp
        test_array_bitmap_index.cpp
        test_array_inverted_index.cpp
        test_bf.cpp
        test_bf_sparse.cpp
        test_binary.cpp
        test_binlog_index.cpp
        test_bitmap_index.cpp
        test_bool_index.cpp
        test_c_api.cpp
        test_chunk.cpp
        test_chunk_vector.cpp
        test_common.cpp
        test_concurrent_vector.cpp
        test_c_stream_reduce.cpp
        test_c_tokenizer.cpp
        test_loading.cpp
        test_data_codec.cpp
        test_delete_record.cpp
        test_disk_file_manager_test.cpp
        test_exec.cpp
        test_expr.cpp
        test_expr_materialized_view.cpp
        test_float16.cpp
        test_function.cpp
        test_futures.cpp
        test_group_by.cpp
        test_iterative_filter.cpp
        test_growing.cpp
        test_growing_index.cpp
        test_hybrid_index.cpp
        test_index_c_api.cpp
        test_indexing.cpp
        test_index_wrapper.cpp
        test_init.cpp
        test_integer_overflow.cpp
        test_inverted_index.cpp
        test_local_chunk_manager.cpp
        test_mmap_chunk_manager.cpp
        test_monitor.cpp
        test_offset_ordered_array.cpp
        test_offset_ordered_map.cpp
        test_plan_proto.cpp
        test_query.cpp
        test_range_search_sort.cpp
        test_reduce_c.cpp
        test_reduce.cpp
        test_regex_query.cpp
        test_regex_query_util.cpp
        test_relational.cpp
        test_retrieve.cpp
        test_scalar_index.cpp
        test_sealed.cpp
        test_segcore.cpp
        test_similarity_corelation.cpp
        test_span.cpp
        test_storage.cpp
        test_string_expr.cpp
        test_text_match.cpp
        test_timestamp_index.cpp
        test_tracer.cpp
        test_utils.cpp
        test_chunked_segment.cpp
        test_chunked_column.cpp
        test_rust_result.cpp
        test_cached_search_iterator.cpp
        test_random_sample.cpp
        test_json_index.cpp
        test_json_key_stats_index.cpp
        test_types.cpp
        test_growing_storage_v2.cpp
        test_memory_planner.cpp
        test_storage_v2_index_raw_data.cpp
        test_chunked_column_group.cpp
        test_group_chunk_translator.cpp
        test_chunked_segment_storage_v2.cpp
        )

if ( INDEX_ENGINE STREQUAL "cardinal" )
    set(MILVUS_TEST_FILES
        ${MILVUS_TEST_FILES}
        test_kmeans_clustering.cpp)
endif()

if ( BUILD_DISK_ANN STREQUAL "ON" )
    set(MILVUS_TEST_FILES
            ${MILVUS_TEST_FILES}
            #need update aws-sdk-cpp, see more from https://github.com/aws/aws-sdk-cpp/issues/1757
            #test_minio_chunk_manager.cpp
            )
endif()

if (LINUX OR APPLE)
    set(MILVUS_TEST_FILES
            ${MILVUS_TEST_FILES}
            test_scalar_index_creator.cpp
            test_string_index.cpp
            test_array.cpp
            test_array_expr.cpp)
endif()

if (DEFINED AZURE_BUILD_DIR)
    set(MILVUS_TEST_FILES
                ${MILVUS_TEST_FILES}
                test_azure_chunk_manager.cpp
            #need update aws-sdk-cpp, see more from https://github.com/aws/aws-sdk-cpp/issues/2119
                #test_remote_chunk_manager.cpp
                )
    include_directories("${AZURE_BUILD_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include")
endif()

if (ENABLE_GCP_NATIVE)
    add_definitions(-DENABLE_GCP_NATIVE)
    set(MILVUS_TEST_FILES
                ${MILVUS_TEST_FILES}
                test_gcp_native_chunk_manager.cpp
                )
endif()

if (LINUX)
    message( STATUS "Building Milvus Unit Test on Linux")
    option(USE_ASAN "Whether to use AddressSanitizer" OFF)
    if ( USE_ASAN )
        message( STATUS "Building Milvus using AddressSanitizer")
        add_compile_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
        add_link_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
    endif()

    # check if memory leak exists in index builder
    set(INDEX_BUILDER_TEST_FILES
            test_index_wrapper.cpp
            test_scalar_index_creator.cpp
            test_index_c_api.cpp
            )

    add_executable(index_builder_test
            ${INDEX_BUILDER_TEST_FILES}
            )

    target_link_libraries(index_builder_test
            gtest
            milvus_core
            knowhere
            milvus-storage
            )
    install(TARGETS index_builder_test DESTINATION unittest)
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
        )

install(TARGETS all_tests DESTINATION unittest)

add_subdirectory(bench)
add_subdirectory(test_cachinglayer)

# if (USE_DYNAMIC_SIMD)
# add_executable(dynamic_simd_test
#                test_simd.cpp)
#
# target_link_libraries(dynamic_simd_test
#                 milvus_simd
#                 milvus_log
#                 gtest
#                 ${CONAN_LIBS})
#
# install(TARGETS dynamic_simd_test DESTINATION unittest)
# endif()

add_executable(bitset_test
        test_bitset.cpp
)
target_link_libraries(bitset_test
        milvus_bitset
        gtest
        ${CONAN_LIBS}
)
install(TARGETS bitset_test DESTINATION unittest)
