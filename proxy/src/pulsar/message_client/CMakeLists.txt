set(src-cpp
        Client.cpp
        Consumer.cpp
        Producer.cpp
        pb/suvlim.pb.cc)

add_library(message_client_cpp SHARED
        ${src-cpp}
        )
target_include_directories(message_client_cpp PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/pulsar/pulsar-src/pulsar-client-cpp/include)

target_link_libraries(message_client_cpp pulsarStatic libprotobuf)

#install(TARGETS message_client_cpp
#        DESTINATION lib)