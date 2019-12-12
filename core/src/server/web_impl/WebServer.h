//
// Created by yhz on 2019/12/5.
//

#pragma once

#include "../../utils/Status.h"

#include <oatpp/network/server/Server.hpp>

#include <memory>
#include <string>
#include <thread>

namespace milvus {
namespace server {
namespace web {

class WebServer {
 public:
    static WebServer&
    GetInstance() {
        static WebServer web_server;
        return web_server;
    }

    void
    Start();

    void
    Stop();

 private:
    WebServer();
    ~WebServer();

    Status
    StartService();
    Status
    StopService();

 private:
    std::unique_ptr<oatpp::network::server::Server> server_ptr_;
};

} // namespace web
} // namespace server
} // namespace milvus

