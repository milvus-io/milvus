enable_testing()
set( GRPC_SERVICE_FILES 
	 ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/suvlim.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/suvlim.pb.cc
#        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
#        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
#        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/hello.grpc.pb.cc
#        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/hello.pb.cc
#        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/master.grpc.pb.cc
#        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/master.pb.cc
        )


set(unittest_srcs
        unittest_entry.cpp
        consumer_test.cpp producer_test.cpp)


add_executable(test_pulsar ${unittest_srcs} ${GRPC_SERVICE_FILES})

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
