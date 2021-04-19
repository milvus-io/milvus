include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(${CMAKE_HOME_DIRECTORY}/src/index/knowhere)
include_directories(${CMAKE_HOME_DIRECTORY}/src/thirdparty)

add_definitions(-DMILVUS_TEST_SEGCORE_YAML_PATH="${CMAKE_SOURCE_DIR}/unittest/test_utils/test_segcore.yaml")

set(MILVUS_TEST_FILES
        test_naive.cpp
        test_segcore.cpp
        test_concurrent_vector.cpp
        test_c_api.cpp
        test_indexing.cpp
        test_query.cpp
        test_expr.cpp
        test_bitmap.cpp 
        test_binary.cpp
        test_index_wrapper.cpp
        test_common.cpp
        test_sealed.cpp
        test_reduce.cpp
        test_interface.cpp
        test_span.cpp
        test_load.cpp
        init_gtest.cpp
        test_init.cpp
        )

add_executable(all_tests
        ${MILVUS_TEST_FILES}
        )

# check if memory leak exists in index builder
set(INDEX_BUILDER_TEST_FILES
        test_index_wrapper.cpp
        )

add_executable(index_builder_test
        ${INDEX_BUILDER_TEST_FILES}
        )
target_link_libraries(index_builder_test
        gtest
        gtest_main
        milvus_segcore
        milvus_indexbuilder
        log
        pthread
        )

target_link_libraries(all_tests
        gtest
        milvus_segcore
        milvus_indexbuilder
        log
        pthread
        )

install (TARGETS all_tests DESTINATION unittest)
