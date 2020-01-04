// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <grpcpp/impl/codegen/interceptor.h>
#include <grpcpp/impl/codegen/server_interceptor.h>
#include <opentracing/tracer.h>
#include <memory>

#include "GrpcInterceptorHookHandler.h"

namespace milvus {
namespace server {
namespace grpc {

class SpanInterceptor : public ::grpc::experimental::Interceptor {
 public:
    SpanInterceptor(::grpc::experimental::ServerRpcInfo* info, GrpcInterceptorHookHandler* hook_handler);

    void
    Intercept(::grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
    ::grpc::experimental::ServerRpcInfo* info_;
    GrpcInterceptorHookHandler* hook_handler_;
    //    std::shared_ptr<opentracing::Tracer> tracer_;
    //    std::unique_ptr<opentracing::Span> span_;
};

class SpanInterceptorFactory : public ::grpc::experimental::ServerInterceptorFactoryInterface {
 public:
    explicit SpanInterceptorFactory(GrpcInterceptorHookHandler* hook_handler) : hook_handler_(hook_handler) {
    }

    ::grpc::experimental::Interceptor*
    CreateServerInterceptor(::grpc::experimental::ServerRpcInfo* info) override;

 private:
    GrpcInterceptorHookHandler* hook_handler_;
};

}  // namespace grpc
}  // namespace server
}  // namespace milvus
