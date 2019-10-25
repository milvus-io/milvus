#!/bin/bash

../../cmake-build-debug/grpc_ep-prefix/src/grpc_ep/bins/opt/protobuf/protoc -I . --grpc_out=./gen-status --plugin=protoc-gen-grpc="../../cmake-build-debug/grpc_ep-prefix/src/grpc_ep/bins/opt/grpc_cpp_plugin" status.proto

../../cmake-build-debug/grpc_ep-prefix/src/grpc_ep/bins/opt/protobuf/protoc -I . --cpp_out=./gen-status status.proto

../../cmake-build-debug/grpc_ep-prefix/src/grpc_ep/bins/opt/protobuf/protoc -I . --grpc_out=./gen-milvus --plugin=protoc-gen-grpc="../../cmake-build-debug/grpc_ep-prefix/src/grpc_ep/bins/opt/grpc_cpp_plugin" milvus.proto

../../cmake-build-debug/grpc_ep-prefix/src/grpc_ep/bins/opt/protobuf/protoc -I . --cpp_out=./gen-milvus milvus.proto