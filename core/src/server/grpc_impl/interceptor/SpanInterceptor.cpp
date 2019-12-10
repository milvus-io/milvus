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

#include "server/grpc_impl/interceptor/SpanInterceptor.h"
#include "tracing/TracerUtil.h"

namespace milvus {
namespace server {
namespace grpc {

SpanInterceptor::SpanInterceptor(::grpc::experimental::ServerRpcInfo* info, GrpcInterceptorHookHandler* hook_handler)
    : info_(info), hook_handler_(hook_handler) {
}

void
SpanInterceptor::Intercept(::grpc::experimental::InterceptorBatchMethods* methods) {
    if (methods->QueryInterceptionHookPoint(::grpc::experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
        hook_handler_->OnPostRecvInitialMetaData(info_, methods);

    } else if (methods->QueryInterceptionHookPoint(::grpc::experimental::InterceptionHookPoints::PRE_SEND_MESSAGE)) {
        hook_handler_->OnPreSendMessage(info_, methods);
    }

    methods->Proceed();
}

::grpc::experimental::Interceptor*
SpanInterceptorFactory::CreateServerInterceptor(::grpc::experimental::ServerRpcInfo* info) {
    return new SpanInterceptor(info, hook_handler_);
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
