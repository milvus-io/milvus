enable_testing()
find_package(GTest REQUIRED)

include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(>>>> ${CMAKE_HOME_DIRECTORY}/src/index/knowhere)
set(MILVUS_TEST_FILES
        test_naive.cpp
        test_dog_segment.cpp
        test_concurrent_vector.cpp
        test_c_api.cpp
        test_indexing.cpp
        )
add_executable(all_tests
        ${MILVUS_TEST_FILES}
        )

target_link_libraries(all_tests
        gtest
        gtest_main
        milvus_dog_segment
        knowhere
        log
        pthread
        )
install (TARGETS all_tests DESTINATION unittest)