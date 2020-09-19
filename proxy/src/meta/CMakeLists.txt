include_directories(${PROJECT_BINARY_DIR}/thirdparty/grpc/grpc-src/third_party/protobuf/src)
include_directories(${PROJECT_BINARY_DIR}/thirdparty/grpc/grpc-src/include)

add_subdirectory( etcd_watcher )
aux_source_directory( ./master  master_src)
aux_source_directory(./etcd_client etcd_src)
add_library(meta
        ${master_src}
        ${etcd_src}
        ./etcd_watcher/Watcher.cpp
        ${PROJECT_SOURCE_DIR}/src/grpc/etcd.pb.cc
        ${PROJECT_SOURCE_DIR}/src/grpc/etcd.grpc.pb.cc
        ${PROJECT_SOURCE_DIR}/src/grpc/master.pb.cc
        ${PROJECT_SOURCE_DIR}/src/grpc/master.grpc.pb.cc
        )
