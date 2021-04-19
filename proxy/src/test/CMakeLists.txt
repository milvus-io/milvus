AUX_SOURCE_DIRECTORY(. TEST)

set( GRPC_SERVICE_FILES ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/hello.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/hello.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/master.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/master.pb.cc
        )
add_executable(test_pulsar ${TEST} ${GRPC_SERVICE_FILES})

target_include_directories(test_pulsar PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/pulsar/pulsar-src/pulsar-client-cpp/include)
target_include_directories(test_pulsar PUBLIC ${PROJECT_BINARY_DIR}/thirdparty/avro/avro-build/include)


target_link_libraries(test_pulsar
        pulsarStatic
        libprotobuf
        grpc++_reflection
        grpc++
        libboost_system.a
        libboost_filesystem.a
        libboost_serialization.a
        )