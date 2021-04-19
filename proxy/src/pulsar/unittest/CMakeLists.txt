enable_testing()
set(unittest_srcs
        unittest_entry.cpp
        consumer_test.cpp producer_test.cpp)

add_executable(test ${unittest_srcs})

target_link_libraries(test
        message_client_cpp
        pulsar
        gtest
        gtest_main)

install(TARGETS test DESTINATION unittest)