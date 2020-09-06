set(src-cpp
        Consumer.cpp
        Producer.cpp
        Client.cpp
        pb/suvlim.pb.cc
        pb/status.pb.cc)

add_library(client_cpp SHARED
        ${src-cpp}
        )

target_link_libraries(client_cpp pulsar protobuf)

install(TARGETS client_cpp
        DESTINATION lib)