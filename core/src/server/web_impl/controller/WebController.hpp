//
//  UserController.hpp
//  web-starter-project
//
//  Created by Leonid on 2/12/18.
//  Copyright © 2018 oatpp. All rights reserved.
//

#pragma once

#include <string>
#include <iostream>

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/parser/json/mapping/ObjectMapper.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include "../dto/ResultDto.hpp"
#include "../handler/WebHandler.h"

namespace milvus {
namespace server {
namespace web {

/**
 *  EXAMPLE ApiController
 *  Basic examples of howto create ENDPOINTs
 *  More details on oatpp.io
 */
class WebController : public oatpp::web::server::api::ApiController {
 public:
    WebController(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper))
        : oatpp::web::server::api::ApiController(objectMapper) {}

 private:

    /**
     *  Inject Database component
     */
    OATPP_COMPONENT(std::shared_ptr<WebHandler>, handler_);
 public:

    /**
     *  Inject @objectMapper component here as default parameter
     *  Do not return bare Controllable* object! use shared_ptr!
     */
    static std::shared_ptr<WebController> createShared(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>,
                                                                       objectMapper)) {
        return std::make_shared<WebController>(objectMapper);
    }

    /**
     *  Begin ENDPOINTs generation ('ApiController' codegen)
     */
#include OATPP_CODEGEN_BEGIN(ApiController)

    /**
     *  Insert Your endpoints here !!!
     */

    ENDPOINT_INFO(root) {
        info->summary = "Index.html page";
        info->addResponse<HasTableDto::ObjectWrapper>(Status::CODE_200, "text/html");
    }

    ENDPOINT("GET", "/", root) {
        const char* html =
            "<html lang='en'>"
            "<head>"
            "<meta charset=utf-8/>"
            "</head>"
            "<body>"
            "<p>Hello CRUD example project!</p>"
            //    "<a href='swagger/ui'>Checkout Swagger-UI page</a>"
            "</body>"
            "</html>";
        auto response = createResponse(Status::CODE_200, html);
        response->putHeader(Header::CONTENT_TYPE, "text/html");
        return response;
    }

    ENDPOINT_INFO(hasTable) {
        info->summary = "Check if has table";
        info->addConsumes<HasTableDto::ObjectWrapper>("application/json");
        info->addResponse<HasTableDto::ObjectWrapper>(Status::CODE_200, "application/json");

    }
    // ENDPOINT("<http-method>", "<path>", <method-name>, <optional param-mappings>)
    ENDPOINT("GET", "demo/{tableName}", hasTable,
             PATH(String, tableName)) {
        return createDtoResponse(Status::CODE_200, handler_->hasTable(std::string(tableName->c_str())));
    }

    ENDPOINT("GET", "/users", getUsers,
             QUERIES(
                 const QueryParams&, queryParams)) {
        for (auto& param : queryParams.getAll()) {
//          OATPP_LOGD("param", "%s=%s", param.first.getData(), param.second.getData());
            std::cout << "param" << param.first.getData() << "=" << param.second.getData() << std::endl;
        }
        return createResponse(Status::CODE_200, "OK");
    }

    /**
     *  Finish ENDPOINTs generation ('ApiController' codegen)
     */
#include OATPP_CODEGEN_END(ApiController)

};

} // namespace web
} // namespace server
} // namespace milvus