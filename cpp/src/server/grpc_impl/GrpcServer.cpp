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

#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "server/grpc_impl/GrpcServer.h"
#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "GrpcRequestHandler.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

namespace zilliz {
namespace milvus {
namespace server {
namespace grpc {

constexpr int64_t MESSAGE_SIZE = -1;

//this class is to check port occupation during server start
class NoReusePortOption : public ::grpc::ServerBuilderOption {
 public:
    void UpdateArguments(::grpc::ChannelArguments *args) override {
        args->SetInt(GRPC_ARG_ALLOW_REUSEPORT, 0);
    }

    void UpdatePlugins(std::vector<std::unique_ptr<::grpc::ServerBuilderPlugin>> *plugins) override {
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
    Config &config = Config::GetInstance();
    std::string address, port;
    Status s;

    s = config.GetServerConfigAddress(address);
    if (!s.ok()) {
        return s;
    }
    s = config.GetServerConfigPort(port);
    if (!s.ok()) {
        return s;
    }

    std::string server_address(address + ":" + port);

    ::grpc::ServerBuilder builder;
    builder.SetOption(std::unique_ptr<::grpc::ServerBuilderOption>(new NoReusePortOption));
    builder.SetMaxReceiveMessageSize(MESSAGE_SIZE); //default 4 * 1024 * 1024
    builder.SetMaxSendMessageSize(MESSAGE_SIZE);

    builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_STREAM_GZIP, true);
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_STREAM_GZIP);
    builder.SetDefaultCompressionLevel(GRPC_COMPRESS_LEVEL_HIGH);

    GrpcRequestHandler service;

    builder.AddListeningPort(server_address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

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

} // namespace grpc
} // namespace server
} // namespace milvus
} // namespace zilliz
