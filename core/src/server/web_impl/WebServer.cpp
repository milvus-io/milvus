//
// Created by yhz on 2019/12/5.
//

#include <oatpp/network/server/Server.hpp>

#include "server/web_impl/WebServer.h"
#include "server/web_impl/component/AppComponent.hpp"
#include "server/web_impl/controller/WebController.hpp"


namespace milvus {
namespace server {
namespace web {

WebServer::WebServer() {
    oatpp::base::Environment::init();
}

WebServer::~WebServer() {
    oatpp::base::Environment::destroy();
}

void
WebServer::Start() {
    StartService();
}

void
WebServer::Stop() {
    StopService();
}

Status
WebServer::StartService() {
    AppComponent components; // Create scope Environment components
    /* create ApiControllers and add endpoints to router */
    auto router = components.httpRouter.getObject();

    auto userController = WebController::createShared();
    userController->addEndpointsToRouter(router);
    /* create server */
    server_ptr_ = std::make_unique<oatpp::network::server::Server>(
        components.serverConnectionProvider.getObject(),
        components.serverConnectionHandler.getObject()
    );

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

} // namespace web
} // namespace server
} // namespace milvus
