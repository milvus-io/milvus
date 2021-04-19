#!/usr/bin/env bash

../../../../cmake-build-debug/thirdparty/grpc/grpc-build/third_party/protobuf/protoc -I=./ --cpp_out=./ pulsar.proto