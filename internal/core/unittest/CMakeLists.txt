enable_testing()

include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(${CMAKE_HOME_DIRECTORY}/src/index/knowhere)
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
        test_sealed.cpp
        test_reduce.cpp
        )
add_executable(all_tests
        ${MILVUS_TEST_FILES}
        )

target_link_libraries(all_tests
        gtest
        gtest_main
        milvus_segcore
        milvus_indexbuilder
        log
        pthread
        )

install (TARGETS all_tests DESTINATION unittest)
