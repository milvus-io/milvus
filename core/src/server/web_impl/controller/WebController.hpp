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

#include "server/web_impl/handler/WebHandler.h"

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
     *  Inject handler
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

    /*
     * Root
     */
    ENDPOINT_INFO(root) {
        info->summary = "Index.html page";
        info->addResponse<String>(Status::CODE_200, "text/html");
    }

    ENDPOINT("GET", "/", root) {
        const char* html =
            "<html lang='en'>"
            "<head>"
            "<meta charset=utf-8/>"
            "</head>"
            "<body>"
            "<p>Hello milvus project!</p>"
            "<a href='swagger/ui'>Checkout Swagger-UI page</a>"
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
     *
     * response body:
     *
     */
    ENDPOINT_INFO(createTable) {
        info->summary = "Create table";
        info->addConsumes<TableRequestDto::ObjectWrapper>("application/json");

        // Created.
        info->addResponse<TableFieldsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("POST", "/tables", createTable,
             BODY_DTO(TableRequestDto::ObjectWrapper, table_schema)) {
        auto status_dto = StatusDto::createShared();
        handler_->CreateTable(table_schema, status_dto);
        if (0 != status_dto->code) {
            return createDtoResponse(Status::CODE_400, status_dto);
        } else {
            return createDtoResponse(Status::CODE_201, status_dto);
        }
    }

    /*
     * Get table
     *
     * url = GET '{server address}/tables/{tableName}?fields={fields list}'
     */
    ENDPOINT_INFO(getTable) {
        info->summary = "Get table";

        // OK.
        info->addResponse<TableFieldsDto::ObjectWrapper>(Status::CODE_200, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        // Table not exists.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("GET", "/tables/{table_name}", getTable,
//             PATH(String, table_name, "table_name"), QUERY(String, fields, "fields")) {
             PATH(String, table_name), QUERIES(const QueryParams&, query_params)) {

        auto fields_dto = TableFieldsDto::createShared();
        auto status_dto = StatusDto::createShared();
        handler_->GetTable(table_name, query_params, status_dto, fields_dto);
        auto code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_200, fields_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Show tables
     *
     * url = GET '<server address>/tables?offset={offset}&page_size={size}'
     */
    ENDPOINT_INFO(showTables) {
        info->summary = "Show whole tables";
        info->addResponse<TableListDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("GET", "/tables", showTables, QUERY(Int64, offset, "offset"), QUERY(Int64, page_size, "page_size")) {
        auto table_list_dto = TableListDto::createShared();
        auto status_dto = StatusDto::createShared();
        handler_->ShowTables(offset, page_size, status_dto, table_list_dto);
        if (0 == status_dto->code->getValue()) {
            return createDtoResponse(Status::CODE_200, table_list_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Drop tables
     *
     * url = DELETE '<server address>/tables/{tableName}'
     */
    ENDPOINT_INFO(dropTable) {
        info->summary = "Drop table";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("DELETE", "tables/{table_name}", dropTable,
             PATH(String, table_name)) {
        auto status_dto = StatusDto::createShared();
        handler_->DropTable(table_name, status_dto);
        auto code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_204, status_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Create index
     *
     * url = POST '<server address>/indexes/tables/<table_name>'
     */
    ENDPOINT_INFO(createIndex) {
        info->summary = "Create index";
        info->addConsumes<IndexRequestDto::ObjectWrapper>("application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("POST", "/indexes/tables/{table_name}", createIndex,
             PATH(String, table_name), BODY_DTO(IndexRequestDto::ObjectWrapper, index_param)) {
        auto status_dto = StatusDto::createShared();
        handler_->CreateIndex(table_name, index_param, status_dto);
        if (0 == status_dto->code->getValue()) {
            return createDtoResponse(Status::CODE_201, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Get index
     *
     * url = GET '<server address>/indexes/tables/{tableName}'
     */
    ENDPOINT_INFO(getIndex) {
        info->summary = "Describe index";
        info->addResponse<IndexDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("GET", "indexes/tables/{tableName}", getIndex,
             PATH(String, table_name)) {
        auto index_dto = IndexDto::createShared();
        auto status_dto = StatusDto::createShared();
        handler_->GetIndex(table_name, status_dto, index_dto);
        auto code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_200, index_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Drop index
     *
     * url = DELETE '<server address>/indexes/tables/{tableName}'
     */
    ENDPOINT_INFO(dropIndex) {
        info->summary = "Drop index";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("DELETE", "indexes/tables/{tableName}", dropIndex, PATH(String, table_name)) {
        auto status_dto = StatusDto::createShared();
        handler_->DropIndex(table_name, status_dto);
        auto code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_204, status_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Create partition
     *
     * url = POST '<server address>/partitions/tables/<table_name>'
     */
    ENDPOINT_INFO(createPartition) {
        info->summary = "Create partition";
        info->addConsumes<PartitionRequestDto::ObjectWrapper>("application/json");

        // Created.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
//        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("POST",
             "/partitions/tables/{tableName}",
             createPartition,
             PATH(String, table_name),
             BODY_DTO(PartitionRequestDto::ObjectWrapper, partition_param)) {

        auto status_dto = StatusDto::createShared();
        handler_->CreatePartition(table_name, partition_param, status_dto);

        if (0 == status_dto->code->getValue()) {
            return createDtoResponse(Status::CODE_201, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Show partitions
     *
     * url = GET '<server address>/partitions/tables/{tableName}?offset={}&page_size={}'
     */
    ENDPOINT_INFO(showPartitions) {
        info->summary = "Show partitions";

        //
        info->addResponse<PartitionListDto::ObjectWrapper>(Status::CODE_200, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        //
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("GET",
             "partitions/tables/{tableName}",
             showPartitions,
             PATH(String, table_name),
             QUERY(Int64, offset, "Page offset"),
             QUERY(Int64, page_size, "Page size")) {
        auto partition_list_dto = PartitionListDto::createShared();
        auto status_dto = StatusDto::createShared();
        handler_->ShowPartitions(offset, page_size, table_name, status_dto, partition_list_dto);
        int64_t code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_200, partition_list_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Drop partition
     *
     * url = DELETE '<server address>/partitions/tables?table_name={tableName}&tag={tag}'
     */
    ENDPOINT_INFO(dropPartition) {
        info->summary = "Drop partition";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("DELETE", "/partitions/tables", dropPartition,
             QUERY(String, table_name, "table-name"), QUERY(String, tag, "partition-tag")) {
        auto status_dto = StatusDto::createShared();
        handler_->DropPartition(table_name, tag, status_dto);
        auto code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_200, status_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Insert vectors
     *
     * url POST '<server addr>/vectors/tables?table_name={}&tag={}'
     */
    ENDPOINT_INFO(insert) {
        info->summary = "Insert vectors";

        info->addConsumes<InsertRequestDto::ObjectWrapper>("application/json");

        info->addResponse<VectorIdsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("POST",
             "/vectors/tables",
             insert,
//             QUERY(String, table_name),
             QUERIES(const QueryParams&, query_params),
             BODY_DTO(InsertRequestDto::ObjectWrapper, insert_param)) {
        auto ids_dto = VectorIdsDto::createShared();
        auto status_dto = StatusDto::createShared();
        handler_->Insert(query_params, insert_param, status_dto, ids_dto);

        int64_t code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_201, status_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * Search
     *
     * url GET 'vectors/tables/{tableName}?topk={topk}&nprobe={nprobe}&tags={tag list}'
     */
    ENDPOINT_INFO(search) {
        info->summary = "Search";

        info->addConsumes<RecordsDto::ObjectWrapper>("application/json");

        info->addResponse<ResultDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("GET", "/vectors/{table_name}", search,
             PATH(String, table_name),
             QUERY(Int64, topk), QUERY(Int64, nprobe), QUERIES(const QueryParams&, query_params),
             BODY_DTO(RecordsDto::ObjectWrapper, records)) {
        auto result_dto = ResultDto::createShared();
        auto status_dto = StatusDto::createShared();
        handler_->Search(table_name, topk, nprobe, query_params, records, status_dto, result_dto);
        int64_t code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_200, result_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == code || milvus::DB_NOT_FOUND == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    /*
     * cmd
     *
     * url = GET '<server address>/cmd/{cmd_str}'
     */
    ENDPOINT_INFO(cmd) {
        info->summary = "Command";
        info->addResponse<CommandDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
//        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("GET", "/cmd/{cmd_str}", cmd, PATH(String, cmd_str)) {
        auto cmd_dto = CommandDto::createShared();
        auto status_dto = StatusDto::createShared();
        handler_->Cmd(cmd_str, status_dto, cmd_dto);
        if (0 == status_dto->code->getValue()) {
            return createDtoResponse(Status::CODE_200, cmd_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }
    /**
     *  Finish ENDPOINTs generation ('ApiController' codegen)
     */
#include OATPP_CODEGEN_END(ApiController)

};

} // namespace web
} // namespace server
} // namespace milvus
