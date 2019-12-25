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

#include "server/web_impl/handler/WebRequestHandler.h"


# define CORS_SUPPORT(RESPONSE)                                                                 \
    do {                                                                                        \
        response->putHeader("access-control-allow-methods", "GET, POST, PUT, OPTIONS, DELETE"); \
        response->putHeader("access-control-allow-origin", "*");                                \
        response->putHeader("access-control-allow-headers",                                     \
            "DNT, User-Agent, X-Requested-With, If-Modified-Since, "                            \
            "Cache-Control, Content-Type, Range, Authorization");                               \
        response->putHeader("access-control-max-age", "1728000");                               \
    } while(false);


namespace milvus {
namespace server {
namespace web {

class WebController : public oatpp::web::server::api::ApiController {
 public:
    WebController(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper))
        : oatpp::web::server::api::ApiController(objectMapper) {}

 private:

    /**
     *  Inject web handler
     */
    OATPP_COMPONENT(std::shared_ptr<WebRequestHandler>, handler_);
 public:

    static std::shared_ptr<WebController> createShared(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>,
                                                                       objectMapper)) {
        return std::make_shared<WebController>(objectMapper);
    }

    /**
     *  Begin ENDPOINTs generation ('ApiController' codegen)
     */
#include OATPP_CODEGEN_BEGIN(ApiController)

    ENDPOINT_INFO(root) {
        info->summary = "Index.html page";
        info->addResponse<String>(Status::CODE_200, "text/html");
    }

    ENDPOINT("GET", "/", root) {
        auto response = createResponse(Status::CODE_302, "Welcome to milvus");
        response->putHeader(Header::CONTENT_TYPE, "text/html");
        // redirect to swagger ui
        response->putHeader("location", "/swagger/ui");
        return response;
    }

    ENDPOINT_INFO(State) {
        info->summary = "Server state";
        info->description = "Check web server whether is ready.";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }

    ENDPOINT("GET", "/state", State) {
        auto response = createDtoResponse(Status::CODE_200, StatusDto::createShared());
        CORS_SUPPORT(response)
        return response;
    }

    ENDPOINT("OPTIONS", "/tables", TablesOptions, REQUEST(const std::shared_ptr<IncomingRequest>&, request)) {
        auto response = createDtoResponse(Status::CODE_200, StatusDto::createShared());
        CORS_SUPPORT(response)
        return response;
    }

    ENDPOINT_INFO(CreateTable) {
        info->summary = "Create table";

        info->addConsumes<TableRequestDto::ObjectWrapper>("application/json");

        // Created.
        info->addResponse<TableFieldsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("POST", "/tables", CreateTable,
             BODY_DTO(TableRequestDto::ObjectWrapper, table_schema)) {
        auto status_dto = handler_->CreateTable(table_schema);
        std::shared_ptr<OutgoingResponse> response;
        if (0 != status_dto->code) {
            response = createDtoResponse(Status::CODE_400, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_201, status_dto);
        }

        CORS_SUPPORT(response);
        return response;
    }

    ENDPOINT_INFO(ShowTables) {
        info->summary = "Show whole tables";
        info->addResponse<TableListFieldsDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("GET", "/tables", ShowTables, QUERY(Int64, offset, "offset"), QUERY(Int64, page_size, "page_size")) {
        auto response_dto = TableListFieldsDto::createShared();
        auto status_dto = handler_->ShowTables(offset, page_size, response_dto);
        std::shared_ptr<OutgoingResponse> response;
        if (0 == status_dto->code->getValue()) {
            response = createDtoResponse(Status::CODE_200, response_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response);
        return response;
    }

    ENDPOINT("OPTIONS", "/tables/{table_name}", TableOptions, PATH(String, table_name)) {
        auto status_dto = StatusDto::createShared();
        status_dto->code = 0;
        status_dto->message = "OK";

        auto response = createDtoResponse(Status::CODE_200, status_dto);

        CORS_SUPPORT(response);
        return response;
    }

    ENDPOINT_INFO(GetTable) {
        info->summary = "Get table";
        info->description = "";

        // OK.
        info->addResponse<TableFieldsDto::ObjectWrapper>(Status::CODE_200, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        // Table not exists.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("GET", "/tables/{table_name}", GetTable,
             PATH(String, table_name), QUERIES(
                 const QueryParams&, query_params)) {

        auto fields_dto = TableFieldsDto::createShared();
        auto status_dto = handler_->GetTable(table_name, query_params, fields_dto);
        std::shared_ptr<OutgoingResponse> response;
        auto code = status_dto->code->getValue();
        if (0 == code) {
            response = createDtoResponse(Status::CODE_200, fields_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
            response = createDtoResponse(Status::CODE_404, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response);

        return response;
    }

    ENDPOINT_INFO(DropTable) {
        info->summary = "Drop table";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT("DELETE", "/tables/{table_name}", DropTable,
             PATH(String, table_name), REQUEST(const std::shared_ptr<IncomingRequest>&, request)) {
        auto status_dto = handler_->DropTable(table_name);
        auto code = status_dto->code->getValue();
        std::shared_ptr<OutgoingResponse> response;
        if (0 == code) {
            response = createDtoResponse(Status::CODE_204, status_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
            response = createDtoResponse(Status::CODE_404, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response);

        return response;
    }

    ENDPOINT("OPTIONS", "/tables/{table_name}/indexes", IndexOptions, REQUEST(const std::shared_ptr<IncomingRequest>&, request)) {
        auto response = createDtoResponse(Status::CODE_200, StatusDto::createShared());
        CORS_SUPPORT(response)
        return response;
    }

    ENDPOINT_INFO(CreateIndex) {
        info->summary = "Create index";
        info->addConsumes<IndexRequestDto::ObjectWrapper>("application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT("POST", "/tables/{table_name}/indexes", CreateIndex,
             PATH(String, table_name), BODY_DTO(IndexRequestDto::ObjectWrapper, index_param)) {
        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler_->CreateIndex(table_name, index_param);
        if (0 == status_dto->code->getValue()) {
            response = createDtoResponse(Status::CODE_201, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response);

        return response;
    }

    ENDPOINT_INFO(GetIndex) {
        info->summary = "Describe index";
        info->addResponse<IndexDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(GetIndex)
    ENDPOINT("GET", "/tables/{table_name}/indexes", GetIndex,
             PATH(String, table_name)) {
        auto index_dto = IndexDto::createShared();
        auto status_dto = handler_->GetIndex(table_name, index_dto);
        auto code = status_dto->code->getValue();
        std::shared_ptr<OutgoingResponse> response;
        if (0 == code) {
            response = createDtoResponse(Status::CODE_200, index_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
            response =  createDtoResponse(Status::CODE_404, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response)

        return response;
    }

    ENDPOINT_INFO(DropIndex) {
        info->summary = "Drop index";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(DropIndex)
    ENDPOINT("DELETE", "/tables/{table_name}/indexes", DropIndex, PATH(String, table_name)) {
        auto status_dto = handler_->DropIndex(table_name);
        auto code = status_dto->code->getValue();
        std::shared_ptr<OutgoingResponse> response;
        if (0 == code) {
            response = createDtoResponse(Status::CODE_204, status_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
            response = createDtoResponse(Status::CODE_404, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response);

        return response;
    }

    /*
     * Create partition
     *
     * url = POST '<server address>/partitions/tables/<table_name>'
     */
    ENDPOINT("OPTIONS", "/tables/{table_name}/partitions", PartitionsOptions, REQUEST(const std::shared_ptr<IncomingRequest>&, request)) {
        auto response = createDtoResponse(Status::CODE_200, StatusDto::createShared());
        CORS_SUPPORT(response)
        return response;
    }

    ENDPOINT_INFO(CreatePartition) {
        info->summary = "Create partition";
        info->addConsumes<PartitionRequestDto::ObjectWrapper>("application/json");

        // Created.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
//        info->addResponse<String>(Status::CODE_404, "text/plain");
    }

    ADD_CORS(CreatePartition)
    ENDPOINT("POST",
             "/tables/{table_name}/partitions",
             CreatePartition,
             PATH(String, table_name),
             BODY_DTO(PartitionRequestDto::ObjectWrapper, partition_param)) {

        auto status_dto = handler_->CreatePartition(table_name, partition_param);
        std::shared_ptr<OutgoingResponse> response;

        if (0 == status_dto->code->getValue()) {
            response = createDtoResponse(Status::CODE_201, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response)

        return response;
    }

    /*
     * Show partitions
     *
     * url = GET '<server address>/partitions/tables/{tableName}?offset={}&page_size={}'
     */
    ENDPOINT_INFO(ShowPartitions) {
        info->summary = "Show partitions";

        //
        info->addResponse<PartitionListDto::ObjectWrapper>(Status::CODE_200, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        //
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(ShowPartitions)
    ENDPOINT("GET",
             "/tables/{tableName}/partitions",
             ShowPartitions,
             PATH(String, table_name),
             QUERY(Int64, offset, "Page offset"),
             QUERY(Int64, page_size, "Page size")) {
        auto partition_list_dto = PartitionListDto::createShared();
        auto status_dto = handler_->ShowPartitions(offset, page_size, table_name, partition_list_dto);
        int64_t code = status_dto->code->getValue();
        std::shared_ptr<OutgoingResponse> response;
        if (0 == code) {
            response = createDtoResponse(Status::CODE_200, partition_list_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
            response = createDtoResponse(Status::CODE_404, status_dto);
        } else {
            response = createDtoResponse(Status::CODE_400, status_dto);
        }

        CORS_SUPPORT(response)
        return response;
    }

    ENDPOINT("OPTIONS", "/tables/{table_name}/partition/{tag}", PartitionOptions, REQUEST(const std::shared_ptr<IncomingRequest>&, request)) {
        auto response = createDtoResponse(Status::CODE_200, StatusDto::createShared());
        CORS_SUPPORT(response)
        return response;    }

    ENDPOINT_INFO(DropPartition) {
        info->summary = "Drop partition";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(DropPartition)
    ENDPOINT("DELETE", "/tables/{table_name}/partition/{tag}", DropPartition,
             QUERY(String, table_name), QUERY(String, tag)) {
        auto status_dto = handler_->DropPartition(table_name, tag);
        auto code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_204, status_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    ENDPOINT_INFO(Insert) {
        info->summary = "Insert vectors";

        info->addConsumes<InsertRequestDto::ObjectWrapper>("application/json");

        info->addResponse<VectorIdsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(Insert)
    ENDPOINT("POST",
             "/vectors/tables",
             Insert,
            BODY_DTO(InsertRequestDto::ObjectWrapper, insert_param)) {
        auto ids_dto = VectorIdsDto::createShared();
        auto status_dto = handler_->Insert(insert_param, ids_dto);

        int64_t code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_201, status_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
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
    ENDPOINT_INFO(Search) {
        info->summary = "Search";

        info->addConsumes<RecordsDto::ObjectWrapper>("application/json");

        info->addResponse<ResultDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(Search)
    ENDPOINT("GET", "/vectors/{table_name}", Search,
             PATH(String, table_name),
             QUERY(Int64, topk), QUERY(Int64, nprobe), QUERIES(
                 const QueryParams&, query_params),
             BODY_DTO(RecordsDto::ObjectWrapper, records)) {
        auto result_dto = ResultDto::createShared();
        auto status_dto = handler_->Search(table_name, topk, nprobe, query_params, records, result_dto);
        int64_t code = status_dto->code->getValue();
        if (0 == code) {
            return createDtoResponse(Status::CODE_200, result_dto);
        } else if (StatusCode::TABLE_NOT_EXISTS == code) {
            return createDtoResponse(Status::CODE_404, status_dto);
        } else {
            return createDtoResponse(Status::CODE_400, status_dto);
        }
    }

    ENDPOINT_INFO(Cmd) {
        info->summary = "Command";
        info->addResponse<CommandDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(Cmd)
    ENDPOINT("GET", "/cmd/{cmd_str}", Cmd, PATH(String, cmd_str)) {
        auto cmd_dto = CommandDto::createShared();
        auto status_dto = handler_->Cmd(cmd_str, cmd_dto);
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
