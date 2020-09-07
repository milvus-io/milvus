set(src-cpp
        Client.cpp
        ClientV2.cpp
        Consumer.cpp
        Producer.cpp
        M3_hash.cpp
        PartitionPolicy.cpp)

add_library(message_client_cpp
        ${src-cpp}
        )

target_include_directories(message_client_cpp PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/pulsar/pulsar-src/pulsar-client-cpp/include)

target_link_libraries(message_client_cpp utils pulsarStatic libprotobuf)

#install(TARGETS message_client_cpp
#        DESTINATION lib)
