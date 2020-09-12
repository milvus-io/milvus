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

#include <iostream>

#include <oatpp/core/macro/component.hpp>
#include <oatpp/network/client/SimpleTCPConnectionProvider.hpp>
#include <oatpp/network/server/SimpleTCPConnectionProvider.hpp>
#include <oatpp/parser/json/mapping/Deserializer.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/parser/json/mapping/Serializer.hpp>
#include <oatpp/web/server/HttpConnectionHandler.hpp>
#include <oatpp/web/server/HttpRouter.hpp>

namespace milvus {
namespace server {
namespace web {

class AppComponent {

 public:

    explicit AppComponent(int port) : port_(port) {
    }

 private:
    const int port_;

 public:
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ServerConnectionProvider>, server_connection_provider_)
    ([this] {
        try {
            return oatpp::network::server::SimpleTCPConnectionProvider::createShared(this->port_);
        } catch (std::exception& e) {
            std::string error_msg = "Cannot bind http port " + std::to_string(this->port_) + ". " + e.what() +
                "(errno: " + std::to_string(errno) + ", details: " + strerror(errno) + ")";
            std::cerr << error_msg << std::endl;
            throw std::runtime_error(error_msg);
        }
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ClientConnectionProvider>, client_connection_provider_)
    ([this] {
        return oatpp::network::client::SimpleTCPConnectionProvider::createShared("localhost", this->port_);
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, http_router_)([] {
        return oatpp::web::server::HttpRouter::createShared();
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::server::ConnectionHandler>, server_connection_handler_)([] {
        OATPP_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, router);
        return oatpp::web::server::HttpConnectionHandler::createShared(router);
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::data::mapping::ObjectMapper>, api_object_mapper_)([] {
        auto serializerConfig = oatpp::parser::json::mapping::Serializer::Config::createShared();
        auto deserializerConfig = oatpp::parser::json::mapping::Deserializer::Config::createShared();
        deserializerConfig->allowUnknownFields = false;
        return oatpp::parser::json::mapping::ObjectMapper::createShared(serializerConfig, deserializerConfig);
    }());
};

} //namespace web
} //namespace server
} //namespace milvus
