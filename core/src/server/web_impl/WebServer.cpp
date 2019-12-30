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

#include <oatpp-swagger/AsyncController.hpp>
#include <oatpp/network/server/Server.hpp>

#include "server/web_impl/WebServer.h"
#include "server/web_impl/controller/WebController.hpp"
#include "server/web_impl/component/SwaggerComponent.hpp"

#include "server/Config.h"

namespace milvus {
namespace server {
namespace web {

WebServer::WebServer() {
}

WebServer::~WebServer() {
}

void
WebServer::Start() {
    thread_ptr_ = std::make_shared<std::thread>(&WebServer::StartService, this);
}

void
WebServer::Stop() {
    StopService();

    if (thread_ptr_ != nullptr) {
        thread_ptr_->join();
        thread_ptr_ = nullptr;
    }
}

Status
WebServer::StartService() {
    Config& config = Config::GetInstance();
    std::string port;
    Status status;

    status = config.GetServerConfigWebPort(port);
    AppComponent components = AppComponent(std::stoi(port));
    SwaggerComponent swaggerComponent("192.168.1.57", std::stoi(port));

    /* create ApiControllers and add endpoints to router */
    auto router = components.http_router_.getObject();

    auto doc_endpoints = oatpp::swagger::AsyncController::Endpoints::createShared();

    auto user_controller = WebController::createShared();
    user_controller->addEndpointsToRouter(router);

    doc_endpoints->pushBackAll(user_controller->getEndpoints());

    auto swaggerController = oatpp::swagger::AsyncController::createShared(doc_endpoints);
    swaggerController->addEndpointsToRouter(router);

    /* create server */
    server_ptr_ = std::make_unique<oatpp::network::server::Server>(components.server_connection_provider_.getObject(),
                                                                   components.server_connection_handler_.getObject());

    // start synchronously
    server_ptr_->run();

    return Status::OK();
}

Status
WebServer::StopService() {
    if (server_ptr_ != nullptr) {
        server_ptr_->stop();
    }
    return Status::OK();
}

}  // namespace web
}  // namespace server
}  // namespace milvus
