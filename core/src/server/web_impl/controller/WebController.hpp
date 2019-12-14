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

#include <string>
#include <iostream>

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/parser/json/mapping/ObjectMapper.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include "server/web_impl/dto/TableDto.hpp"
#include "server/web_impl/dto/CmdDto.hpp"
#include "server/web_impl/dto/IndexDto.hpp"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"

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
     *  Web routing
     */

    ENDPOINT_INFO(root) {
        info->summary = "Index.html page";
//        info->addResponse<HasTableDto::ObjectWrapper>(Status::CODE_200, "text/html");
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

    /*
     * Create table
     *
     * url = POST '<server address>/tables'
     */
    ENDPOINT_INFO(createTable) {
        info->summary = "Create table";
        info->addConsumes<TableSchemaDto::ObjectWrapper>("application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("POST", "tables", createTable,
        BODY_DTO(TableSchemaDto::ObjectWrapper, tableSchema)) {
        return createDtoResponse(Status::CODE_200, handler_->CreateTable(tableSchema));
    }

    /*
     * Has table
     *
     * url = GET '<server address>/tables/state/{tableName}'
     */
    ENDPOINT_INFO(hasTable) {
        info->summary = "Check if has table";
        info->addConsumes<TableNameDto::ObjectWrapper>("application/json");
        info->addResponse<BoolReplyDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }
//     ENDPOINT("<http-method>", "<path>", <method-name>, <optional param-mappings>)
    ENDPOINT("GET", "tables/state/{tableName}", hasTable,
             PATH(String, tableName)) {
        return createDtoResponse(Status::CODE_200, handler_->hasTable(tableName->std_str()));
    }

    /*
     * Describe table
     *
     * url = GET '{server address}/tables/{tableName}'
     */
    ENDPOINT_INFO(getTable) {
        info->summary = "Describe table";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("GET", "tables/{tableName}", getTable,
            PATH(String, tableName)) {
        return createDtoResponse(Status::CODE_200, handler_->DescribeTable(tableName->std_str()));
    }

    /*
     * Count table
     *
     * url = GET 'tables/number/{tableName}'
     */
    ENDPOINT_INFO(countTable) {
        info->summary = "Obtain table raw record count";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("GET", "tables/number/{tableName}", countTable,
        PATH(String, tableName)) {
        return createDtoResponse(Status::CODE_200, handler_->CountTable(tableName->std_str()));
    }

    /*
     * Show tables
     *
     * url = GET '<server address>/tables?pageId=<id>'
     */
    ENDPOINT_INFO(showTables) {
        info->summary = "Show whole tables";
        info->addResponse<TableNameListDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("GET", "tables", showTables, QUERY(Int64, pageId, "page-id")) {
        return createDtoResponse(Status::CODE_200, handler_->ShowTables());
    }

    /*
     * Drop tables
     *
     * url = DELETE '<server address>/tables/{tableName}'
     */
    ENDPOINT_INFO(dropTable) {
        info->summary = "Drop table";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("DELETE", "tables/{tableName}", dropTable,
        PATH(String, tableName)) {
        return createDtoResponse(Status::CODE_200, handler_->DropTable(tableName->std_str()));
    }

    /*
     * Create index
     *
     * url = POST '<server address>/indexes'
     */
    ENDPOINT_INFO(createIndex) {
        info->summary = "Create index";
        info->addConsumes<IndexRequestDto::ObjectWrapper>("application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }
    ENDPOINT("POST", "indexes", createIndex, BODY_DTO(IndexParamDto::ObjectWrapper, indexParam)) {

    }

    /*
     * Describe index
     *
     * url = GET '<server address>/indexes/{tableName}'
     */
    ENDPOINT_INFO(describeIndex) {
        info->summary = "Describe index";
        info->addResponse<IndexParamDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }
    ENDPOINT("GET", "indexes/{tableName}", describeIndex, PATH(String, tableName)) {

    }

    /*
     * Drop index
     *
     * url = DELETE '<server address>/indexes/{tableName}'
     */
    ENDPOINT_INFO(dropIndex) {
        info->summary = "Drop index";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }
    ENDPOINT("DELETE", "indexes/{tableName}", dropIndex, PATH(String, tableName)) {

    }

    /*
     * Create partition
     *
     * url = POST '<server address>/partitions'
     */
    ENDPOINT_INFO(createPartition) {
        info->summary = "Create partition";
        info->addConsumes<PartitionParamDto::ObjectWrapper>("application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }
    ENDPOINT("POST", "partitions/", createPartition, BODY_DTO(PartitionParamDto::ObjectWrapper, partitionParam)) {

    }

    /*
     * Show partitions
     *
     * url = GET '<server address>/partitions/{tableName}?pageId={id}'
     */
    ENDPOINT("GET", "partitions/{tableName}", showPartitions, PATH(String, tableName)) {

    }

    /*
     * Drop partition
     *
     * url = DELETE '<server address>/partitions/{tableName}?tag={tag}'
     */
    ENDPOINT("DELETE", "partitions/{tableName}", dropPartition,
        PATH(String, tableName), QUERY(String, tag, "partition-tag")) {

    }

    /*
     * Insert vectors
     */

    /*
     * Search
     */

    /*
     * cmd
     *
     * url = GET '<server address>/cmd/{cmd_str}'
     */
    ENDPOINT("GET", "cmd/{cmd_str}", cmd, PATH(String, cmd_str)) {

    }
//    ENDPOINT("GET", "/users", getUsers,
//             QUERIES(
//                 const QueryParams&, queryParams)) {
//        for (auto& param : queryParams.getAll()) {
//          OATPP_LOGD("param", "%s=%s", param.first.getData(), param.second.getData());
//            std::cout << "param" << param.first.getData() << "=" << param.second.getData() << std::endl;
//        }
//        return createResponse(Status::CODE_200, "OK");
//    }

    /**
     *  Finish ENDPOINTs generation ('ApiController' codegen)
     */
#include OATPP_CODEGEN_END(ApiController)

};

} // namespace web
} // namespace server
} // namespace milvus