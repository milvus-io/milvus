set(src-cpp
        client.cpp
        consumer.cpp
        producer.cpp
        pb/pulsar.pb.cc)

add_library(client_cpp SHARED
        ${src-cpp}
        )

target_link_libraries(client_cpp pulsar protobuf)

install(TARGETS client_cpp
        DESTINATION lib)