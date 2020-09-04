#!/bin/bash

../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --grpc_out=./gen-status --plugin=protoc-gen-grpc="../../cmake-build-debug/thirdparty/grpc/grpc-build/grpc_cpp_plugin" status.proto

../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --cpp_out=./gen-status status.proto

#../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --grpc_out=./gen-milvus --plugin=protoc-gen-grpc="../../cmake-build-debug/thirdparty/grpc/grpc-build/grpc_cpp_plugin" milvus.proto
#
#../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --cpp_out=./gen-milvus milvus.proto
#
#
#../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --grpc_out=./gen-milvus --plugin=protoc-gen-grpc="../../cmake-build-debug/thirdparty/grpc/grpc-build/grpc_cpp_plugin" hello.proto
#
#../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --cpp_out=./gen-milvus hello.proto
#
#../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --grpc_out=./gen-milvus --plugin=protoc-gen-grpc="../../cmake-build-debug/thirdparty/grpc/grpc-build/grpc_cpp_plugin" master.proto
#
#../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --cpp_out=./gen-milvus master.proto

# TODO: spilt milvus and pulsar proto
../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --grpc_out=./gen-milvus --plugin=protoc-gen-grpc="../../cmake-build-debug/thirdparty/grpc/grpc-build/grpc_cpp_plugin" suvlim.proto

../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I . --cpp_out=./gen-milvus suvlim.proto