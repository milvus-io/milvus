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
             BODY_DTO(TableRequestDto::ObjectWrapper, tableSchema)) {
        auto status = handler_->CreateTable(tableSchema);
        if (status.code() != 0) {
            return createResponse(Status::CODE_400, String(status.message().c_str()));
        }

        return createResponse(Status::CODE_201, "Created Successfully.");
    }

    /*
     * Get table
     *
     * url = GET '{server address}/tables/{tableName}[?fields={fields list}]'
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

    ENDPOINT("GET", "/tables/{tableName}", getTable,
             PATH(String, tableName)) {

        auto fields_dto = TableFieldsDto::createShared();

        String fields;
        auto status = handler_->GetTable(tableName, fields, fields_dto);
        // TODO: check status
        if (milvus::DB_SUCCESS == status.code()) {
            return createDtoResponse(Status::CODE_200, fields_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404, "Table " + tableName + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
        }
    }

    /*
     * Show tables
     *
     * url = GET '<server address>/tables?offset={offset}&page_size={size}'
     */
    ENDPOINT_INFO(showTables) {
        info->summary = "Show whole tables";
        info->addResponse<TableNameListDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("GET", "/tables", showTables, QUERY(Int64, offset, "offset"), QUERY(Int64, page_size, "page size")) {
        auto table_list_dto = TableNameListDto::createShared();
        auto status = handler_->ShowTables(table_list_dto);
        if (status.ok()) {
            return createDtoResponse(Status::CODE_200, table_list_dto);
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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

    ENDPOINT("DELETE", "tables/{tableName}", dropTable,
             PATH(String, tableName)) {
        auto status = handler_->DropTable(tableName);
        if (status.ok()) {
            return createResponse(Status::CODE_204, "Delete successfully.");
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404, "Table " + tableName + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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
        PATH(String, table_name, "Table name"), BODY_DTO(IndexRequestDto::ObjectWrapper, indexParam)) {
        auto status = handler_->CreateIndex(indexParam);
        if (status.ok()) {
            return createResponse(Status::CODE_201, status.message().c_str());
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("GET", "indexes/tables/{tableName}", getIndex,
        PATH(String, tableName)) {
        auto index_dto = IndexDto::createShared();
        auto status = handler_->GetIndex(tableName, index_dto);

        if (status.ok()) {
            return createDtoResponse(Status::CODE_200, index_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404, "Table " + tableName + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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

    ENDPOINT("DELETE", "indexes/tables/{tableName}", dropIndex, PATH(String, tableName)) {
        auto status = handler_->DropIndex(tableName);
        if (status.ok()) {
            return createResponse(Status::CODE_204, "Delete successfully.");
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404, "Table " + tableName + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
        }
    }

    /*
     * Create partition
     *
     * url = POST '<server address>/partitions/tables/<table_name>'
     */
    ENDPOINT_INFO(createPartition) {
        info->summary = "Create partition";
        info->addConsumes<PartitionParamDto::ObjectWrapper>("application/json");

        // Created.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
//        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("POST",
             "/partitions/tables/{tableName}",
             createPartition,
             PATH(String, tableName),
             BODY_DTO(PartitionParamDto::ObjectWrapper, partitionParam)) {
        auto param = PartitionParamDto::createShared();
        auto status = handler_->CreatePartition(param);

        if (status.ok()) {
            return createResponse(Status::CODE_201, "Create successfully.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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
             PATH(String, tableName),
             QUERY(Int64, offset, "Page offset"),
             QUERY(Int64, page_size, "Page size")) {
        auto partition_list_dto = PartitionListDto::createShared();
        auto status = handler_->ShowPartitions(tableName, partition_list_dto);

        if (status.ok()) {
            return createDtoResponse(Status::CODE_200, partition_list_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404, "Table " + tableName + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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
        auto status = handler_->DropPartition(table_name, tag);

        if (status.ok()) {
            return createResponse(Status::CODE_200, "Delete successfully.");
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404, "Table " + table_name + "\' tag " + tag + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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

        // TODO: may return ids
        info->addResponse<VectorIdsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<String>(Status::CODE_400, "text/plain");
        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ENDPOINT("POST", "/vectors/tables", insert,
             QUERY(String, table_name, "Table name"), QUERY(String, tag, "partition-tag"), BODY_DTO(InsertRequestDto::ObjectWrapper, insertParam)) {
        auto ids_dto = VectorIdsDto::createShared();
        auto status = handler_->Insert(table_name, insertParam, ids_dto);

        if (status.ok()) {
            return createResponse(Status::CODE_201, "Insert successfully.");
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404,
                                  "Table " + table_name + "\' tag " + insertParam->tag + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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

    ENDPOINT("GET", "/vectors/{tableName}", search,
             PATH(String, tableName), 
             QUERY(Int64, topk, "top-k"), QUERY(Int64, nprobe, "num of probe"), QUERY(String, tags, "tag list"),
             BODY_DTO(RecordsDto::ObjectWrapper, records)) {
        auto result_dto = ResultDto::createShared();
        auto status = handler_->Search(tableName, topk, nprobe, tags, records, result_dto);
        if (status.ok()) {
            return createDtoResponse(Status::CODE_200, result_dto);
        } else if (milvus::SERVER_TABLE_NOT_EXIST == status.code() || milvus::DB_NOT_FOUND == status.code()) {
            return createResponse(Status::CODE_404,
                                  "Table " + tableName + " not found.");
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
        }
    }

    /*
     * cmd
     *
     * url = GET '<server address>/cmd/{cmd_str}'
     */
    ENDPOINT_INFO(cmd) {
        info->summary = "Command";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("GET", "/cmd/{cmd_str}", cmd, PATH(String, cmd_str)) {
        String reply;
        auto status = handler_->Cmd(cmd_str, reply);
        if (status.ok()) {
            return createResponse(Status::CODE_200, reply);
        } else {
            return createResponse(Status::CODE_400, status.message().c_str());
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
