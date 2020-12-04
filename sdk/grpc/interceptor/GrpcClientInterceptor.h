// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "grpc/interceptor/GrpcInterceptorHookHandler.h"

namespace milvus {

class GrpcClientInterceptor : public ::grpc::experimental::Interceptor {
 public:
    explicit GrpcClientInterceptor(::grpc::experimental::ClientRpcInfo* info, GrpcInterceptorHookHandler* hook_handler);

    void
    Intercept(::grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
    ::grpc::experimental::ClientRpcInfo* info_ = nullptr;
    GrpcInterceptorHookHandler* hook_handler_;
};

class GrpcClientInterceptorFactory : public ::grpc::experimental::ClientInterceptorFactoryInterface {
 public:
    explicit GrpcClientInterceptorFactory(GrpcInterceptorHookHandler* hook_handler) : hook_handler_(hook_handler){};

    ::grpc::experimental::Interceptor*
    CreateClientInterceptor(::grpc::experimental::ClientRpcInfo* info) override;

 private:
    GrpcInterceptorHookHandler* hook_handler_;
};

}  // namespace milvus
