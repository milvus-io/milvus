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

#include <oatpp-swagger/Controller.hpp>
#include <oatpp/network/server/Server.hpp>

#include "server/web_impl/WebServer.h"
#include "server/web_impl/component/AppComponent.hpp"
#include "server/web_impl/controller/WebController.hpp"

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
    //    oatpp::base::Environment::init();
    StartService();
}

void
WebServer::Stop() {
    StopService();
    //    oatpp::base::Environment::destroy();
}

Status
WebServer::StartService() {
    Config& config = Config::GetInstance();
    std::string port;
    Status status;

    status = config.GetServerConfigWebPort(port);
    AppComponent components(atoi(port.c_str()));

    /* create ApiControllers and add endpoints to router */
    auto router = components.httpRouter.getObject();

    auto doc_endpoints = oatpp::swagger::Controller::Endpoints::createShared();

    auto user_controller = WebController::createShared();
    user_controller->addEndpointsToRouter(router);

    doc_endpoints->pushBackAll(user_controller->getEndpoints());

    auto swaggerController = oatpp::swagger::Controller::createShared(doc_endpoints);
    swaggerController->addEndpointsToRouter(router);

    /* create server */
    server_ptr_ = std::make_unique<oatpp::network::server::Server>(components.serverConnectionProvider.getObject(),
                                                                   components.serverConnectionHandler.getObject());

    // start asynchronously
    server_ptr_->run();

    return Status::OK();
}

Status
WebServer::StopService() {
    if (server_ptr_) {
        server_ptr_->stop();
    }
    return Status::OK();
}

}  // namespace web
}  // namespace server
}  // namespace milvus
