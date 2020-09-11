enable_testing()
find_package(GTest REQUIRED)
set(MILVUS_TEST_FILES
    test_naive.cpp
    test_dog_segment.cpp
    test_concurrent_vector.cpp
    test_c_api.cpp
)
add_executable(all_tests 
    ${MILVUS_TEST_FILES}
)

target_link_libraries(all_tests 
    gtest 
    gtest_main
    milvus_dog_segment
    pthread
)