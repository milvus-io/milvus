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

#include "server/grpc_impl/GrpcServer.h"

#include <grpc++/grpc++.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "GrpcRequestHandler.h"
#include "config/Config.h"
#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "server/DBWrapper.h"
#include "server/grpc_impl/interceptor/SpanInterceptor.h"
#include "utils/Log.h"

namespace milvus {
namespace server {
namespace grpc {

constexpr int64_t MESSAGE_SIZE = -1;

// this class is to check port occupation during server start
class NoReusePortOption : public ::grpc::ServerBuilderOption {
 public:
    void
    UpdateArguments(::grpc::ChannelArguments* args) override {
        args->SetInt(GRPC_ARG_ALLOW_REUSEPORT, 0);
        args->SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS, 20);
    }

    void
    UpdatePlugins(std::vector<std::unique_ptr<::grpc::ServerBuilderPlugin>>* plugins) override {
    }
};

void
GrpcServer::Start() {
    thread_ptr_ = std::make_shared<std::thread>(&GrpcServer::StartService, this);
}

void
GrpcServer::Stop() {
    StopService();
    if (thread_ptr_) {
        thread_ptr_->join();
        thread_ptr_ = nullptr;
    }
}

Status
GrpcServer::StartService() {
    SetThreadName("grpcserv_thread");
    Config& config = Config::GetInstance();
    std::string address, port;

    STATUS_CHECK(config.GetServerConfigAddress(address));
    STATUS_CHECK(config.GetServerConfigPort(port));

    std::string server_address(address + ":" + port);

    ::grpc::ServerBuilder builder;
    builder.SetOption(std::unique_ptr<::grpc::ServerBuilderOption>(new NoReusePortOption));
    builder.SetMaxReceiveMessageSize(MESSAGE_SIZE);  // default 4 * 1024 * 1024
    builder.SetMaxSendMessageSize(MESSAGE_SIZE);

    builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_STREAM_GZIP, true);
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_STREAM_GZIP);
    builder.SetDefaultCompressionLevel(GRPC_COMPRESS_LEVEL_NONE);

    GrpcRequestHandler service(opentracing::Tracer::Global());
    service.RegisterRequestHandler(RequestHandler());

    builder.AddListeningPort(server_address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    // Add gRPC interceptor
    using InterceptorI = ::grpc::experimental::ServerInterceptorFactoryInterface;
    using InterceptorIPtr = std::unique_ptr<InterceptorI>;
    std::vector<InterceptorIPtr> creators;

    creators.push_back(
        std::unique_ptr<::grpc::experimental::ServerInterceptorFactoryInterface>(new SpanInterceptorFactory(&service)));

    builder.experimental().SetInterceptorCreators(std::move(creators));

    server_ptr_ = builder.BuildAndStart();
    server_ptr_->Wait();

    return Status::OK();
}

Status
GrpcServer::StopService() {
    if (server_ptr_ != nullptr) {
        server_ptr_->Shutdown();
    }

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
