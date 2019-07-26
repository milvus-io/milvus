#!/bin/bash

protoc -I . --grpc_out=./gen-status --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` status.proto

protoc -I . --cpp_out=./gen-status status.proto

protoc -I . --grpc_out=./gen-milvus --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` milvus.proto

protoc -I . --cpp_out=./gen-milvus milvus.proto