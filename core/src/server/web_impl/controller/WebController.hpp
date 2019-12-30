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

#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>

#include "server/web_impl/dto/ConfigDto.hpp"
#include "server/web_impl/dto/TableDto.hpp"
#include "server/web_impl/dto/CmdDto.hpp"
#include "server/web_impl/dto/IndexDto.hpp"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"
#include "server/web_impl/dto/ConfigDto.hpp"

#include "server/delivery/RequestHandler.h"
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

// private:
//
//    /**
//     *  Inject web handler
//     */
//    OATPP_COMPONENT(std::shared_ptr<WebRequestHandler>, handler_);
 public:

    static std::shared_ptr<WebController> createShared(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>,
                                                                       objectMapper)) {
        return std::make_shared<WebController>(objectMapper);
    }

//    std::shared_ptr<WebRequestHandler> GetRequestHandler() {
//        return handler_;
//    }

    /**
     *  Begin ENDPOINTs generation ('ApiController' codegen)
     */
#include OATPP_CODEGEN_BEGIN(ApiController)

    ENDPOINT_INFO(root) {
        info->summary = "Index.html page";
        info->addResponse<String>(Status::CODE_200, "text/html");
    }

    ENDPOINT_ASYNC("GET", "/", root) {
     ENDPOINT_ASYNC_INIT(root);
        Action
        act() override {
            auto response = controller->createResponse(Status::CODE_302, "Welcome to milvus");
            response->putHeader(Header::CONTENT_TYPE, "text/html");
            // redirect to swagger ui
            response->putHeader("location", "/swagger/ui");
            return _return(response);
        }
    };

    ENDPOINT_INFO(State) {
        info->summary = "Server state";
        info->description = "Check web server whether is ready.";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }

    ENDPOINT_ASYNC("GET", "/state", State) {
     ENDPOINT_ASYNC_INIT(State);

        Action act() {
            auto response = controller->createDtoResponse(Status::CODE_200, StatusDto::createShared());
            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_ASYNC("GET", "/devices", GetDevices) {
     ENDPOINT_ASYNC_INIT(GetDevices);

        Action
        act() override {
            auto devices_dto = DevicesDto::createShared();
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.GetDevices(devices_dto);
            std::shared_ptr<OutgoingResponse> response;
            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_200, devices_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_200, status_dto);
            }

            CORS_SUPPORT(response)

            return _return(response);
        }
    };

    ENDPOINT_ASYNC("OPTIONS", "/config/advanced", AdvancedConfigOptions) {
     ENDPOINT_ASYNC_INIT(AdvancedConfigOptions);

        Action
        act() override {
            auto response = controller->createDtoResponse(Status::CODE_200, StatusDto::createShared());
            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_INFO(GetAdvancedConfig) {
        info->summary = "";

        info->addResponse<AdvancedConfigDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT_ASYNC("GET", "/config/advanced", GetAdvancedConfig) {
     ENDPOINT_ASYNC_INIT(GetAdvancedConfig);

        Action
        act() override {
            auto config_dto = AdvancedConfigDto::createShared();
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.GetAdvancedConfig(config_dto);
            std::shared_ptr<OutgoingResponse> response;
            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_200, config_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_INFO(SetAdvancedConfig) {
        info->summary = "";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT_ASYNC("PUT",
                   "/config/advanced",
                   SetAdvancedConfig) {
//                   BODY_DTO(AdvancedConfigDto::ObjectWrapper, config_dto)) {
     ENDPOINT_ASYNC_INIT(SetAdvancedConfig);

        Action
        act() override {
            return request->readBodyToDtoAsync<AdvancedConfigDto>(controller->getDefaultObjectMapper())
                .callbackTo(&SetAdvancedConfig::returnResponse);
        }

        Action returnResponse(const AdvancedConfigDto::ObjectWrapper& body) {
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());

            auto status_dto = handler.SetAdvancedConfig(body);
            std::shared_ptr<OutgoingResponse> response;
            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_200, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }
            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_ASYNC("OPTIONS", "config/gpu_resources", GPUConfigOptions) {
     ENDPOINT_ASYNC_INIT(GPUConfigOptions);

        Action
        act() override {
            auto response = controller->createDtoResponse(Status::CODE_200, StatusDto::createShared());
            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_ASYNC("GET", "config/gpu_resources", GetGPUConfig) {
     ENDPOINT_ASYNC_INIT(GetGPUConfig);

        Action
        act() override {
            std::shared_ptr<OutgoingResponse> response;

            auto gpu_config_dto = GPUConfigDto::createShared();
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.GetGpuConfig(gpu_config_dto);

            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_200, gpu_config_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_ASYNC("PUT", "config/gpu_resources", SetGPUConfig) {
//    BODY_DTO(GPUConfigDto::ObjectWrapper, gpu_config_dto)) {
     ENDPOINT_ASYNC_INIT(SetGPUConfig);
        Action
        act() override {
            return request->readBodyToDtoAsync<GPUConfigDto>(controller->getDefaultObjectMapper())
                .callbackTo(&SetGPUConfig::returnResponse);
        }

        Action returnResponse(const GPUConfigDto::ObjectWrapper& body) {
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.SetGpuConfig(body);

            std::shared_ptr<OutgoingResponse> response;
            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_200, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_ASYNC("OPTIONS", "/tables", TablesOptions) {
     ENDPOINT_ASYNC_INIT(TablesOptions);

        Action
        act() override {
            auto response = controller->createDtoResponse(Status::CODE_200, StatusDto::createShared());
            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_INFO(CreateTable) {
        info->summary = "Create table";

        info->addConsumes<TableRequestDto::ObjectWrapper>("application/json");

        info->addResponse<TableFieldsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT_ASYNC("POST", "/tables", CreateTable) {
     ENDPOINT_ASYNC_INIT(CreateTable);

        Action
        act()
        override {
            return request->readBodyToDtoAsync<TableRequestDto>(controller->getDefaultObjectMapper())
                .callbackTo(&CreateTable::returnResponse);
        }

        Action returnResponse(const TableRequestDto::ObjectWrapper& body) {
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());

            auto status_dto = handler.CreateTable(body);
            std::shared_ptr<OutgoingResponse> response;
            if (0 != status_dto->code) {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_201, status_dto);
            }

            CORS_SUPPORT(response);
            return _return(response);
        }
    };

    ENDPOINT_INFO(ShowTables) {
        info->summary = "Show whole tables";

//        info->queryParams.add<Int64>("offset");
        info->queryParams.add<Int64>("page_size");

        info->addResponse<TableListFieldsDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT_ASYNC("GET", "/tables", ShowTables) {
     ENDPOINT_ASYNC_INIT(ShowTables);

        Action
        act()
        override {
            auto error_status_dto = StatusDto::createShared();
            String offset_str = request->getQueryParameter("offset");
            if (nullptr == offset_str.get()) {
                error_status_dto->code = StatusCode::QUERY_PARAM_LOSS;
                error_status_dto->message = "Query param \'offset\' is required!";
                return _return(controller->createDtoResponse(Status::CODE_400, error_status_dto));
            }
            Int64 offset = std::stol(offset_str->std_str());

            String page_size_str = request->getQueryParameter("page_size");
            if (nullptr == page_size_str.get()) {
                error_status_dto->code = StatusCode::QUERY_PARAM_LOSS;
                error_status_dto->message = "Query param \'page_size\' is required!";
                return _return(controller->createDtoResponse(Status::CODE_400, error_status_dto));
            }
            Int64 page_size = std::stol(page_size_str->std_str());

            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto response_dto = TableListFieldsDto::createShared();
            auto status_dto = handler.ShowTables(offset, page_size, response_dto);
            std::shared_ptr<OutgoingResponse> response;
            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_200, response_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response);
            return _return(response);
        }
    };

    ENDPOINT_ASYNC("OPTIONS", "/tables/{table_name}", TableOptions) {
     ENDPOINT_ASYNC_INIT(TableOptions);

        Action
        act() override {
            auto status_dto = StatusDto::createShared();
            status_dto->code = 0;
            status_dto->message = "OK";

            auto response = controller->createDtoResponse(Status::CODE_200, status_dto);

            CORS_SUPPORT(response);
            return _return(response);
        }
    };

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

    ENDPOINT_ASYNC("GET", "/tables/{table_name}", GetTable) {
     ENDPOINT_ASYNC_INIT(GetTable);

        Action
        act() override {
            auto error_status_dto = StatusDto::createShared();
            auto table_name = request->getPathVariable("table_name");
            if (nullptr == table_name.get()) {
                error_status_dto->code = StatusCode::QUERY_PARAM_LOSS;
                error_status_dto->message = "Path param \'table_name\' is required!";
                return _return(controller->createDtoResponse(Status::CODE_400, error_status_dto));
            }

            auto query_params = request->getQueryParameters();

            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto fields_dto = TableFieldsDto::createShared();
            auto status_dto = handler.GetTable(table_name, query_params, fields_dto);
            std::shared_ptr<OutgoingResponse> response;
            auto code = status_dto->code->getValue();
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_200, fields_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response);
            return _return(response);
        };
    };

    ENDPOINT_INFO(DropTable) {
        info->summary = "Drop table";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT_ASYNC("DELETE", "/tables/{table_name}", DropTable) {
     ENDPOINT_ASYNC_INIT(DropTable);

        Action
        act() override {
            auto table_name = request->getPathVariable("table_name");
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.DropTable(table_name);
            auto code = status_dto->code->getValue();
            std::shared_ptr<OutgoingResponse> response;
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_204, status_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response);
            return _return(response);
        }
    };

    ENDPOINT_ASYNC("OPTIONS", "/tables/{table_name}/indexes", IndexOptions) {
     ENDPOINT_ASYNC_INIT(IndexOptions);

        Action
        act() override {
            auto response = controller->createDtoResponse(Status::CODE_200, StatusDto::createShared());
            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_INFO(CreateIndex) {
        info->summary = "Create index";
        info->addConsumes<IndexRequestDto::ObjectWrapper>("application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT_ASYNC("POST", "/tables/{table_name}/indexes", CreateIndex) {
     ENDPOINT_ASYNC_INIT(CreateIndex);

        Action
        act() override {
            return request->readBodyToDtoAsync<IndexRequestDto>(controller->getDefaultObjectMapper())
                .callbackTo(&CreateIndex::requestResponse);
        }

        Action requestResponse(const IndexRequestDto::ObjectWrapper& body) {
            auto table_name = request->getPathVariable("table_name");
            auto handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.CreateIndex(table_name, body);

            std::shared_ptr<OutgoingResponse> response;
            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_201, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response);
            return _return(response);
        }
    };

    ENDPOINT_INFO(GetIndex) {
        info->summary = "Describe index";
        info->addResponse<IndexDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT_ASYNC("GET", "/tables/{table_name}/indexes", GetIndex) {
     ENDPOINT_ASYNC_INIT(GetIndex);

        Action
        act() override {
            auto table_name = request->getPathVariable("table_name");
            auto index_dto = IndexDto::createShared();
            auto handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.GetIndex(table_name, index_dto);
            auto code = status_dto->code->getValue();
            std::shared_ptr<OutgoingResponse> response;
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_200, index_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)

            return _return(response);
        }
    };

    ENDPOINT_INFO(DropIndex) {
        info->summary = "Drop index";
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(DropIndex)

    ENDPOINT_ASYNC("DELETE", "/tables/{table_name}/indexes", DropIndex) {
     ENDPOINT_ASYNC_INIT(DropIndex);
        Action
        act()
        override {
            auto handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto table_name = request->getPathVariable("table_name");
            auto status_dto = handler.DropIndex(table_name);
            auto code = status_dto->code->getValue();
            std::shared_ptr<OutgoingResponse> response;
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_204, status_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response);
            return _return(response);
        }
    };

/*
 * Create partition
 *
 * url = POST '<server address>/partitions/tables/<table_name>'
 */
    ENDPOINT("OPTIONS", "/tables/{table_name}/partitions", PartitionsOptions, REQUEST(
        const std::shared_ptr<IncomingRequest>&, request)) {
        auto response = createDtoResponse(Status::CODE_200, StatusDto::createShared());
        CORS_SUPPORT(response)
        return response;
    }

    ENDPOINT_INFO(CreatePartition) {
        info->summary = "Create partition";

        info->addConsumes<PartitionRequestDto::ObjectWrapper>("application/json");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ENDPOINT_ASYNC("POST",
                   "/tables/{table_name}/partitions",
                   CreatePartition) {
//             PATH(String, table_name),
//             BODY_DTO(PartitionRequestDto::ObjectWrapper, partition_param)) {

     ENDPOINT_ASYNC_INIT(CreatePartition);

        Action
        act() override {
            return request->readBodyToDtoAsync<PartitionRequestDto>(controller->getDefaultObjectMapper())
                .callbackTo(&CreatePartition::returnResponse);
        }

        Action returnResponse(const PartitionRequestDto::ObjectWrapper& body) {
            auto handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto table_name = request->getPathVariable("table_name");
            auto status_dto = handler.CreatePartition(table_name, body);
            std::shared_ptr<OutgoingResponse> response;

            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_201, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

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

    ENDPOINT_ASYNC("GET", "/tables/{tableName}/partitions", ShowPartitions) {
     ENDPOINT_ASYNC_INIT(ShowPartitions);
        Action
        act() override {
            auto partition_list_dto = PartitionListDto::createShared();
            auto handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto table_name = request->getPathVariable("table_name");
            Int64 page_size = std::stol(request->getQueryParameter("page_size")->std_str());
            Int64 offset = std::stol(request->getQueryParameter("offset")->std_str());
            auto status_dto = handler.ShowPartitions(offset, page_size, table_name, partition_list_dto);
            int64_t code = status_dto->code->getValue();
            std::shared_ptr<OutgoingResponse> response;
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_200, partition_list_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT("OPTIONS", "/tables/{table_name}/partition/{tag}", PartitionOptions, REQUEST(
        const std::shared_ptr<IncomingRequest>&, request)) {
        auto response = createDtoResponse(Status::CODE_200, StatusDto::createShared());
        CORS_SUPPORT(response)
        return response;
    }

    ENDPOINT_INFO(DropPartition) {
        info->summary = "Drop partition";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT_ASYNC("DELETE", "/tables/{table_name}/partition/{tag}", DropPartition) {
//             QUERY(String, table_name), QUERY(String, tag)) {
     ENDPOINT_ASYNC_INIT(DropPartition);

        Action
        act() override {
            auto handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto table_name = request->getPathVariable("table_name");
            auto tag = request->getPathVariable("tag");
            auto status_dto = handler.DropPartition(table_name, tag);
            auto code = status_dto->code->getValue();
            std::shared_ptr<OutgoingResponse> response;
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_204, status_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_INFO(Insert) {
        info->summary = "Insert vectors";

        info->addConsumes<InsertRequestDto::ObjectWrapper>("application/json");

        info->addResponse<VectorIdsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ENDPOINT_ASYNC("POST", "/vectors/tables", Insert) {
     ENDPOINT_ASYNC_INIT(Insert);

        Action
        act() override {
//             BODY_DTO(InsertRequestDto::ObjectWrapper, insert_param)) {
            return request->readBodyToDtoAsync<InsertRequestDto>(controller->getDefaultObjectMapper())
                .callbackTo(&Insert::returnResponse);
        }

        Action returnResponse(const InsertRequestDto::ObjectWrapper& body) {
            auto ids_dto = VectorIdsDto::createShared();
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.Insert(body, ids_dto);

            std::shared_ptr<OutgoingResponse> response;
            int64_t code = status_dto->code->getValue();
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_201, status_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }

    };

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

    ENDPOINT_ASYNC("GET", "/vectors/{table_name}", Search) {
//             PATH(String, table_name),
//             QUERY(Int64, topk), QUERY(Int64, nprobe), QUERIES(
//                 const QueryParams&, query_params),
//             BODY_DTO(RecordsDto::ObjectWrapper, records)) {
     ENDPOINT_ASYNC_INIT(Search);
        Action
        act() override {
            return request->readBodyToDtoAsync<RecordsDto>(controller->getDefaultObjectMapper())
                .callbackTo(&Search::returnResponse);
        }

        Action returnResponse(const RecordsDto::ObjectWrapper& body) {
            auto result_dto = ResultDto::createShared();
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto table_name = request->getPathVariable("table_name");
            Int64 topk = std::stol(request->getQueryParameter("topk")->std_str());
            Int64 nprobe = std::stol(request->getQueryParameter("nprobe")->std_str());
            auto query_params = request->getQueryParameters();
            auto status_dto = handler.Search(table_name, topk, nprobe, query_params, body, result_dto);
            int64_t code = status_dto->code->getValue();
            std::shared_ptr<OutgoingResponse> response;
            if (0 == code) {
                response = controller->createDtoResponse(Status::CODE_200, result_dto);
            } else if (StatusCode::TABLE_NOT_EXISTS == code) {
                response = controller->createDtoResponse(Status::CODE_404, status_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

    ENDPOINT_INFO(Cmd) {
        info->summary = "Command";
        info->addResponse<CommandDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(Cmd)

    ENDPOINT_ASYNC("GET", "/cmd/{cmd_str}", Cmd) {
     ENDPOINT_ASYNC_INIT(Cmd);

        Action
        act() override {
            auto cmd_str = request->getPathVariable("cmd_str");
            auto cmd_dto = CommandDto::createShared();
            WebRequestHandler handler = WebRequestHandler();
            handler.RegisterRequestHandler(::milvus::server::RequestHandler());
            auto status_dto = handler.Cmd(cmd_str, cmd_dto);
            std::shared_ptr<OutgoingResponse> response;
            if (0 == status_dto->code->getValue()) {
                response = controller->createDtoResponse(Status::CODE_200, cmd_dto);
            } else {
                response = controller->createDtoResponse(Status::CODE_400, status_dto);
            }

            CORS_SUPPORT(response)
            return _return(response);
        }
    };

/**
 *  Finish ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_END(ApiController)

};

} // namespace web
} // namespace server
} // namespace milvus
