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

#include <oatpp/parser/json/mapping/Serializer.hpp>
#include <oatpp/parser/json/mapping/Deserializer.hpp>
#include <oatpp/web/server/AsyncHttpConnectionHandler.hpp>
#include <oatpp/web/server/HttpRouter.hpp>
#include <oatpp/network/server/SimpleTCPConnectionProvider.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/core/macro/component.hpp>

#include "server/web_impl/handler/WebRequestHandler.h"
#include "server/web_impl/component/SwaggerComponent.hpp"

/**
 *  Class which creates and holds Application components and registers components in oatpp::base::Environment
 *  Order of components initialization is from top to bottom
 */
namespace milvus {
namespace server {
namespace web {

/**
 *  Class which creates and holds Application components and registers components in oatpp::base::Environment
 *  Order of components initialization is from top to bottom
 */
class AppComponent {

 public:

    explicit AppComponent(int port) : port_(port) {
    }

    ~AppComponent() = default;

 private:
    const int port_;

 public:
   /**
    *  Swagger component
    */
    OATPP_CREATE_COMPONENT(std::shared_ptr<SwaggerComponent>, swagger_component_)([this] {
        return std::make_shared<SwaggerComponent>("192.168.1.57", this->port_);
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::async::Executor>, executor_)([] {
        return std::make_shared<oatpp::async::Executor>(
            9 /* Data-Processing threads */,
            2 /* I/O threads */,
            1 /* Timer threads */
        );
    }());


    /**
     *  Create ConnectionProvider component which listens on the port
     */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ServerConnectionProvider>, server_connection_provider_)([this] {
        return oatpp::network::server::SimpleTCPConnectionProvider::createShared(static_cast<v_word16>(this->port_));
    }());

    /**
     *  Create Router component
     */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, http_router_)([] {
        return oatpp::web::server::HttpRouter::createShared();
    }());

    /**
     *  Create ConnectionHandler component which uses Router component to route requests
     */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::server::ConnectionHandler>, server_connection_handler_)([] {
        OATPP_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, router); // get Router component
        OATPP_COMPONENT(std::shared_ptr<oatpp::async::Executor>, executor); // get Async executor component
        return oatpp::web::server::AsyncHttpConnectionHandler::createShared(router, executor);
//        return oatpp::web::server::AsyncHttpConnectionHandler::createShared(router);
    }());

    /**
     *  Create ObjectMapper component to serialize/deserialize DTOs in Contoller's API
     */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::data::mapping::ObjectMapper>, api_object_mapper_)([] {
        auto serializerConfig = oatpp::parser::json::mapping::Serializer::Config::createShared();
        auto deserializerConfig = oatpp::parser::json::mapping::Deserializer::Config::createShared();
        deserializerConfig->allowUnknownFields = false;
        auto objectMapper = oatpp::parser::json::mapping::ObjectMapper::createShared(serializerConfig,
                                                                                     deserializerConfig);
        return objectMapper;
    }());

//    /**
//     *  Create Demo-Database component which stores information about users
//     */
//    OATPP_CREATE_COMPONENT(std::shared_ptr<WebRequestHandler>, web_handler_)([] {
//        std::shared_ptr<WebRequestHandler> web_handler = std::make_shared<WebRequestHandler>();
//        web_handler->RegisterRequestHandler(RequestHandler());
//        return web_handler;
//    }());
};

} //namespace web
} //namespace server
} //namespace milvus
