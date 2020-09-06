enable_testing()
set(unittest_srcs
        unittest_entry.cpp
        consumer_test.cpp producer_test.cpp)

add_executable(test_pulsar ${unittest_srcs})

target_include_directories(test_pulsar PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/pulsar/pulsar-src/pulsar-client-cpp/include)
target_include_directories(test_pulsar PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/avro/avro-build/include)

#add_executable(test_pulsar ${unittest_srcs})

target_link_libraries(test_pulsar
        message_client_cpp
        pulsarStatic
        gtest
        gtest_main
        libprotobuf
        grpc++_reflection
        grpc++
        libboost_system.a
        libboost_filesystem.a
        libboost_serialization.a
        )

install(TARGETS test_pulsar DESTINATION unittest)