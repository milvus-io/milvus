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

#include <grpc/grpc.h>
#include <grpcpp/client_context.h>

#include "GrpcClientInterceptor.h"

namespace milvus {

GrpcClientInterceptor::GrpcClientInterceptor(::grpc::experimental::ClientRpcInfo* info,
                                             GrpcInterceptorHookHandler* hook_handler)
    : info_(info), hook_handler_(hook_handler) {
}

void
GrpcClientInterceptor::Intercept(::grpc::experimental::InterceptorBatchMethods* methods) {
    if (methods->QueryInterceptionHookPoint(::grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
        hook_handler_->OnPreSendMessage(info_, methods);
    }
    methods->Proceed();
}

::grpc::experimental::Interceptor*
GrpcClientInterceptorFactory::CreateClientInterceptor(::grpc::experimental::ClientRpcInfo* info) {
    return new GrpcClientInterceptor(info, hook_handler_);
}

}  // namespace milvus
