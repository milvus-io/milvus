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

#include <string>
#include <iostream>

#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>

#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include "server/web_impl/Constants.h"
#include "server/web_impl/dto/CmdDto.hpp"
#include "server/web_impl/dto/ConfigDto.hpp"
#include "server/web_impl/dto/IndexDto.hpp"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/dto/TableDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"
#include "server/web_impl/handler/WebRequestHandler.h"

namespace milvus {
namespace server {
namespace web {

class WebController : public oatpp::web::server::api::ApiController {
 public:
    WebController(const std::shared_ptr<ObjectMapper>& objectMapper)
        : oatpp::web::server::api::ApiController(objectMapper) {}

 public:
    static std::shared_ptr<WebController> createShared(
        OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper)) {
        return std::make_shared<WebController>(objectMapper);
    }

    /**
     *  Begin ENDPOINTs generation ('ApiController' codegen)
     */
#include OATPP_CODEGEN_BEGIN(ApiController)

    ENDPOINT_INFO(root) {
        info->summary = "Index.html page";
    }

    ADD_CORS(root)

    ENDPOINT("GET", "/", root) {
        auto response = createResponse(Status::CODE_200, "Welcome to milvus");
        response->putHeader(Header::CONTENT_TYPE, "text/plain");
        return response;
    }

    ENDPOINT_INFO(State) {
        info->summary = "Server state";
        info->description = "Check web server whether is ready.";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
    }

    ADD_CORS(State)

    ENDPOINT("GET", "/state", State) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/state\'");
        tr.ElapseFromBegin("Total cost ");
        return createDtoResponse(Status::CODE_200, StatusDto::createShared());
    }

    ENDPOINT_INFO(GetDevices) {
        info->summary = "Obtain system devices info";

        info->addResponse<DevicesDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(GetDevices)

    ENDPOINT("GET", "/devices", GetDevices) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/devices\'");
        tr.RecordSection("Receive request");

        auto devices_dto = DevicesDto::createShared();
        WebRequestHandler handler = WebRequestHandler();
        auto status_dto = handler.GetDevices(devices_dto);
        std::shared_ptr<OutgoingResponse> response;
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, devices_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

    ADD_CORS(AdvancedConfigOptions)

    ENDPOINT("OPTIONS", "/config/advanced", AdvancedConfigOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(GetAdvancedConfig) {
        info->summary = "Obtain cache config and enging config";

        info->addResponse<AdvancedConfigDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(GetAdvancedConfig)

    ENDPOINT("GET", "/config/advanced", GetAdvancedConfig) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/config/advanced\'");
        tr.RecordSection("Received request.");

        auto config_dto = AdvancedConfigDto::createShared();
        WebRequestHandler handler = WebRequestHandler();
        auto status_dto = handler.GetAdvancedConfig(config_dto);

        std::shared_ptr<OutgoingResponse> response;
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, config_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

    ENDPOINT_INFO(SetAdvancedConfig) {
        info->summary = "Modify cache config and enging config";

        info->addConsumes<AdvancedConfigDto::ObjectWrapper>("application/json");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(SetAdvancedConfig)

    ENDPOINT("PUT", "/config/advanced", SetAdvancedConfig, BODY_DTO(AdvancedConfigDto::ObjectWrapper, body)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "PUT \'/config/advanced\'");
        tr.RecordSection("Received request.");

        WebRequestHandler handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.SetAdvancedConfig(body);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

#ifdef MILVUS_GPU_VERSION

    ADD_CORS(GPUConfigOptions)

    ENDPOINT("OPTIONS", "/config/gpu_resources", GPUConfigOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(GetGPUConfig) {
        info->summary = "Obtain GPU resources config info";

        info->addResponse<GPUConfigDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(GetGPUConfig)

    ENDPOINT("GET", "/config/gpu_resources", GetGPUConfig) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/config/gpu_resources\'");
        tr.RecordSection("Received request");

        auto gpu_config_dto = GPUConfigDto::createShared();
        WebRequestHandler handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.GetGpuConfig(gpu_config_dto);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, gpu_config_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);

        return response;
    }

    ENDPOINT_INFO(SetGPUConfig) {
        info->summary = "Set GPU resources config";
        info->addConsumes<GPUConfigDto::ObjectWrapper>("application/json");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(SetGPUConfig)

    ENDPOINT("PUT", "/config/gpu_resources", SetGPUConfig, BODY_DTO(GPUConfigDto::ObjectWrapper, body)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "PUT \'/config/gpu_resources\'");
        tr.RecordSection("Received request.");

        WebRequestHandler handler = WebRequestHandler();
        auto status_dto = handler.SetGpuConfig(body);

        std::shared_ptr<OutgoingResponse> response;
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                   + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);
        return response;
    }

#endif

    ADD_CORS(TablesOptions)

    ENDPOINT("OPTIONS", "/tables", TablesOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(CreateTable) {
        info->summary = "Create table";

        info->addConsumes<TableRequestDto::ObjectWrapper>("application/json");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(CreateTable)

    ENDPOINT("POST", "/tables", CreateTable, BODY_DTO(TableRequestDto::ObjectWrapper, body)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "POST \'/tables\'");
        tr.RecordSection("Received request.");

        WebRequestHandler handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.CreateTable(body);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_201, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);
        return response;
    }

    ENDPOINT_INFO(ShowTables) {
        info->summary = "Show whole tables";

        info->queryParams.add<Int64>("offset");
        info->queryParams.add<Int64>("page_size");

        info->addResponse<TableListFieldsDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(ShowTables)

    ENDPOINT("GET", "/tables", ShowTables, QUERIES(const QueryParams&, query_params)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/tables\'");
        tr.RecordSection("Received request.");

        WebRequestHandler handler = WebRequestHandler();

        auto response_dto = TableListFieldsDto::createShared();
        auto offset = query_params.get("offset");
        auto page_size = query_params.get("page_size");

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.ShowTables(offset, page_size, response_dto);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, response_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);

        return response;
    }

    ADD_CORS(TableOptions)

    ENDPOINT("OPTIONS", "/tables/{table_name}", TableOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(GetTable) {
        info->summary = "Get table";

        info->pathParams.add<String>("table_name");

        info->addResponse<TableFieldsDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(GetTable)

    ENDPOINT("GET", "/tables/{table_name}", GetTable,
        PATH(String, table_name), QUERIES(const QueryParams&, query_params)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/tables/" + table_name->std_str() + "\'");
        tr.RecordSection("Received request.");

        WebRequestHandler handler = WebRequestHandler();

        auto fields_dto = TableFieldsDto::createShared();
        auto status_dto = handler.GetTable(table_name, query_params, fields_dto);

        std::shared_ptr<OutgoingResponse> response;
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, fields_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);

        return response;
    }

    ENDPOINT_INFO(DropTable) {
        info->summary = "Drop table";

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(DropTable)

    ENDPOINT("DELETE", "/tables/{table_name}", DropTable, PATH(String, table_name)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "DELETE \'/tables/" + table_name->std_str() + "\'");
        tr.RecordSection("Received request.");

        WebRequestHandler handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.DropTable(table_name);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_204, status_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);

        return response;
    }

    ADD_CORS(IndexOptions)

    ENDPOINT("OPTIONS", "/tables/{table_name}/indexes", IndexOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(CreateIndex) {
        info->summary = "Create index";

        info->addConsumes<IndexRequestDto::ObjectWrapper>("application/json");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(CreateIndex)

    ENDPOINT("POST", "/tables/{table_name}/indexes", CreateIndex,
             PATH(String, table_name), BODY_DTO(IndexRequestDto::ObjectWrapper, body)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "POST \'/tables/" + table_name->std_str() + "/indexes\'");
        tr.RecordSection("Received request.");

        auto handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.CreateIndex(table_name, body);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_201, status_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);

        return response;
    }

    ENDPOINT_INFO(GetIndex) {
        info->summary = "Describe index";

        info->pathParams.add<String>("table_name");

        info->addResponse<IndexDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(GetIndex)

    ENDPOINT("GET", "/tables/{table_name}/indexes", GetIndex, PATH(String, table_name)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/tables/" + table_name->std_str() + "/indexes\'");
        tr.RecordSection("Received request.");

        auto index_dto = IndexDto::createShared();
        auto handler = WebRequestHandler();

        auto status_dto = handler.GetIndex(table_name, index_dto);

        std::shared_ptr<OutgoingResponse> response;
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, index_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);

        return response;
    }

    ENDPOINT_INFO(DropIndex) {
        info->summary = "Drop index";

        info->pathParams.add<String>("table_name");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(DropIndex)

    ENDPOINT("DELETE", "/tables/{table_name}/indexes", DropIndex, PATH(String, table_name)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "DELETE \'/tables/" + table_name->std_str() + "/indexes\'");
        tr.RecordSection("Received request.");

        auto handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.DropIndex(table_name);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_204, status_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        std::string ttr = "Done. Status: code = " + std::to_string(status_dto->code->getValue())
                          + ", reason = " + status_dto->message->std_str() + ". Total cost";
        tr.ElapseFromBegin(ttr);

        return response;
    }

    ADD_CORS(PartitionsOptions)

    ENDPOINT("OPTIONS", "/tables/{table_name}/partitions", PartitionsOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(CreatePartition) {
        info->summary = "Create partition";

        info->pathParams.add<String>("table_name");

        info->addConsumes<PartitionRequestDto::ObjectWrapper>("application/json");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(CreatePartition)

    ENDPOINT("POST", "/tables/{table_name}/partitions",
             CreatePartition, PATH(String, table_name), BODY_DTO(PartitionRequestDto::ObjectWrapper, body)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "POST \'/tables/" + table_name->std_str() + "/partitions\'");
        tr.RecordSection("Received request.");

        auto handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.CreatePartition(table_name, body);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_201, status_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

    ENDPOINT_INFO(ShowPartitions) {
        info->summary = "Show partitions";

        info->pathParams.add<String>("table_name");

        info->queryParams.add<Int64>("offset");
        info->queryParams.add<Int64>("page_size");

        //
        info->addResponse<PartitionListDto::ObjectWrapper>(Status::CODE_200, "application/json");
        // Error occurred.
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        //
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(ShowPartitions)

    ENDPOINT("GET", "/tables/{table_name}/partitions", ShowPartitions,
             PATH(String, table_name), QUERIES(const QueryParams&, query_params)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/tables/" + table_name->std_str() + "/partitions\'");
        tr.RecordSection("Received request.");

        auto offset = query_params.get("offset");
        auto page_size = query_params.get("page_size");

        auto partition_list_dto = PartitionListDto::createShared();
        auto handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.ShowPartitions(offset, page_size, table_name, partition_list_dto);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, partition_list_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

    ADD_CORS(PartitionOptions)

    ENDPOINT("OPTIONS", "/tables/{table_name}/partitions/{partition_tag}", PartitionOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(DropPartition) {
        info->summary = "Drop partition";

        info->pathParams.add<String>("table_name");
        info->pathParams.add<String>("partition_tag");

        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_204, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(DropPartition)

    ENDPOINT("DELETE", "/tables/{table_name}/partitions/{partition_tag}", DropPartition,
             PATH(String, table_name), PATH(String, partition_tag)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) +
                        "DELETE \'/tables/" + table_name->std_str() + "/partitions/" + partition_tag->std_str() + "\'");
        tr.RecordSection("Received request.");

        auto handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.DropPartition(table_name, partition_tag);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_204, status_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

    ADD_CORS(VectorsOptions)

    ENDPOINT("OPTIONS", "/tables/{table_name}/vectors", VectorsOptions) {
        return createResponse(Status::CODE_204, "No Content");
    }

    ENDPOINT_INFO(Insert) {
        info->summary = "Insert vectors";

        info->pathParams.add<String>("table_name");

        info->addConsumes<InsertRequestDto::ObjectWrapper>("application/json");

        info->addResponse<VectorIdsDto::ObjectWrapper>(Status::CODE_201, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(Insert)

    ENDPOINT("POST", "/tables/{table_name}/vectors", Insert,
             PATH(String, table_name), BODY_DTO(InsertRequestDto::ObjectWrapper, body)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "POST \'/tables/" + table_name->std_str() + "/vectors\'");
        tr.RecordSection("Received request.");

        auto ids_dto = VectorIdsDto::createShared();
        WebRequestHandler handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.Insert(table_name, body, ids_dto);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_201, ids_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

    ENDPOINT_INFO(Search) {
        info->summary = "Search";

        info->pathParams.add<String>("table_name");

        info->addConsumes<SearchRequestDto::ObjectWrapper>("application/json");

        info->addResponse<TopkResultsDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_404, "application/json");
    }

    ADD_CORS(Search)

    ENDPOINT("PUT", "/tables/{table_name}/vectors", Search,
             PATH(String, table_name), BODY_DTO(SearchRequestDto::ObjectWrapper, body)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "PUT \'/tables/" + table_name->std_str() + "/vectors\'");
        tr.RecordSection("Received request.");

        auto results_dto = TopkResultsDto::createShared();
        WebRequestHandler handler = WebRequestHandler();

        std::shared_ptr<OutgoingResponse> response;
        auto status_dto = handler.Search(table_name, body, results_dto);
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, results_dto);
                break;
            case StatusCode::TABLE_NOT_EXISTS:
                response = createDtoResponse(Status::CODE_404, status_dto);
                break;
            default:
                response = createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

    ENDPOINT_INFO(SystemMsg) {
        info->summary = "Command";

        info->pathParams.add<String>("cmd_str");

        info->addResponse<CommandDto::ObjectWrapper>(Status::CODE_200, "application/json");
        info->addResponse<StatusDto::ObjectWrapper>(Status::CODE_400, "application/json");
    }

    ADD_CORS(SystemMsg)

    ENDPOINT("GET", "/system/{msg}", SystemMsg, PATH(String, msg), QUERIES(const QueryParams&, query_params)) {
        TimeRecorder tr(std::string(WEB_LOG_PREFIX) + "GET \'/system/" + msg->std_str() + "\'");
        tr.RecordSection("Received request.");

        auto cmd_dto = CommandDto::createShared();
        WebRequestHandler handler = WebRequestHandler();

        auto status_dto = handler.Cmd(msg, query_params, cmd_dto);
        std::shared_ptr<OutgoingResponse> response;
        switch (status_dto->code->getValue()) {
            case StatusCode::SUCCESS:
                response = createDtoResponse(Status::CODE_200, cmd_dto);
                break;
            default:
                return createDtoResponse(Status::CODE_400, status_dto);
        }

        tr.ElapseFromBegin("Done. Status: code = " + std::to_string(status_dto->code->getValue())
                           + ", reason = " + status_dto->message->std_str() + ". Total cost");

        return response;
    }

/**
 *  Finish ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_END(ApiController)

};

} // namespace web
} // namespace server
} // namespace milvus
