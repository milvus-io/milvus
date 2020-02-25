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

#include "utils/Status.h"

#include <grpcpp/grpcpp.h>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace milvus {
namespace server {
namespace grpc {

class GrpcServer {
 public:
    static GrpcServer&
    GetInstance() {
        static GrpcServer grpc_server;
        return grpc_server;
    }

    void
    Start();
    void
    Stop();

 private:
    GrpcServer() = default;
    ~GrpcServer() = default;

    Status
    StartService();
    Status
    StopService();

 private:
    std::unique_ptr<::grpc::Server> server_ptr_;
    std::shared_ptr<std::thread> thread_ptr_;
};

}  // namespace grpc
}  // namespace server
}  // namespace milvus
