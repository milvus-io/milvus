AUX_SOURCE_DIRECTORY(. watcher_src)
add_executable(test_watcher
        ${watcher_src}
        ${PROJECT_SOURCE_DIR}/src/grpc/etcd.pb.cc
        ${PROJECT_SOURCE_DIR}/src/grpc/etcd.grpc.pb.cc
        )

target_link_libraries(
        test_watcher
        PRIVATE
        libprotobuf
        grpc++_reflection
        grpc++
)