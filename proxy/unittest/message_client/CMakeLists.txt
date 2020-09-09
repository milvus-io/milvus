enable_testing()
set( GRPC_SERVICE_FILES 
	 ${MILVUS_ENGINE_SRC}/grpc/message.grpc.pb.cc
		${MILVUS_ENGINE_SRC}/grpc/message.pb.cc
        )


set(unittest_srcs
        unittest_entry.cpp
#        consumer_test.cpp producer_test.cpp
		get_result_test.cpp)


add_executable(test_pulsar ${unittest_srcs} ${GRPC_SERVICE_FILES})

include_directories(${MILVUS_ENGINE_SRC}/utils)

target_include_directories(test_pulsar PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/pulsar/pulsar-src/pulsar-client-cpp/include)
target_include_directories(test_pulsar PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/avro/avro-build/include)

#add_executable(test_pulsar ${unittest_srcs})

target_link_libraries(test_pulsar
		message_client_cpp
		libboost_filesystem.a
		libboost_system.a
		libboost_serialization.a
        gtest
        gtest_main
        libprotobuf
        grpc++_reflection
        grpc++
		pthread
		stdc++
        )

install(TARGETS test_pulsar DESTINATION unittest)
