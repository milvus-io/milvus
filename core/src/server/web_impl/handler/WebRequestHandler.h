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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <opentracing/mocktracer/tracer.h>
#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include "db/Types.h"
#include "server/context/Context.h"
#include "server/delivery/ReqHandler.h"
#include "server/web_impl/Types.h"
#include "server/web_impl/dto/CollectionDto.hpp"
#include "server/web_impl/dto/ConfigDto.hpp"
#include "server/web_impl/dto/DevicesDto.hpp"
#include "server/web_impl/dto/IndexDto.hpp"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"
#include "thirdparty/nlohmann/json.hpp"
#include "utils/Status.h"

namespace milvus {
namespace server {
namespace web {

#define RETURN_STATUS_DTO(STATUS_CODE, MESSAGE)      \
    do {                                             \
        auto status_dto = StatusDto::createShared(); \
        status_dto->code = (STATUS_CODE);            \
        status_dto->message = (MESSAGE);             \
        return status_dto;                           \
    } while (false);

#define ASSIGN_RETURN_STATUS_DTO(STATUS)                    \
    do {                                                    \
        int code;                                           \
        if (0 != (STATUS).code()) {                         \
            code = WebErrorMap((STATUS).code());            \
        } else {                                            \
            code = 0;                                       \
        }                                                   \
        RETURN_STATUS_DTO(code, (STATUS).message().c_str()) \
    } while (false);

StatusCode
WebErrorMap(ErrorCode code);

class WebRequestHandler {
 private:
    std::shared_ptr<Context>
    GenContextPtr(const std::string& context_str) {
        auto context_ptr = std::make_shared<Context>("dummy_request_id");
        opentracing::mocktracer::MockTracerOptions tracer_options;
        auto mock_tracer =
            std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
        auto mock_span = mock_tracer->StartSpan("mock_span");
        auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
        context_ptr->SetTraceContext(trace_context);

        return context_ptr;
    }

 private:
    void
    AddStatusToJson(nlohmann::json& json, int64_t code, const std::string& msg);

    Status
    IsBinaryCollection(const std::string& collection_name, bool& bin);

    Status
    CopyRecordsFromJson(const nlohmann::json& json, std::vector<uint8_t>& vectors_data, bool bin);

    Status
    CopyData2Json(const engine::DataChunkPtr& data_chunk, const engine::snapshot::FieldElementMappings& field_mappings,
                  const std::vector<int64_t>& id_array, nlohmann::json& json_res);

 protected:
    Status
    GetCollectionMetaInfo(const std::string& collection_name, nlohmann::json& json_out);

    Status
    GetCollectionStat(const std::string& collection_name, nlohmann::json& json_out);

    Status
    GetSegmentVectors(const std::string& collection_name, int64_t segment_id, int64_t page_size, int64_t offset,
                      nlohmann::json& json_out);

    Status
    GetPageEntities(const std::string& collection_name, const std::string& partition_tag, const int64_t page_size,
                    const int64_t offset, nlohmann::json& json_out);

    Status
    GetSegmentIds(const std::string& collection_name, int64_t segment_id, int64_t page_size, int64_t offset,
                  nlohmann::json& json_out);

    Status
    CommandLine(const std::string& cmd, std::string& reply);

    Status
    Cmd(const std::string& cmd, std::string& result_str);

    Status
    PreLoadCollection(const nlohmann::json& json, std::string& result_str);

    Status
    Flush(const nlohmann::json& json, std::string& result_str);

    Status
    Compact(const nlohmann::json& json, std::string& result_str);

    Status
    GetConfig(std::string& result_str);

    Status
    SetConfig(const nlohmann::json& json, std::string& result_str);

    Status
    ProcessLeafQueryJson(const nlohmann::json& json, query::BooleanQueryPtr& boolean_query, std::string& field_name,
                         query::QueryPtr& query_ptr);

    Status
    ProcessBooleanQueryJson(const nlohmann::json& query_json, query::BooleanQueryPtr& boolean_query,
                            query::QueryPtr& query_ptr);

    Status
    Search(const std::string& collection_name, const nlohmann::json& json, std::string& result_str);

    Status
    GetEntityByIDs(const std::string& collection_name, const std::vector<int64_t>& ids,
                   std::vector<std::string>& field_names, nlohmann::json& json_out);

 public:
    WebRequestHandler() {
        context_ptr_ = GenContextPtr("Web Handler");
        req_handler_ = ReqHandler();
    }

 public:
    StatusDtoT
    GetDevices(DevicesDtoT& devices);

    StatusDtoT
    GetAdvancedConfig(AdvancedConfigDtoT& config);

    StatusDtoT
    SetAdvancedConfig(const AdvancedConfigDtoT& config);

#ifdef MILVUS_GPU_VERSION
    StatusDtoT
    GetGpuConfig(GPUConfigDtoT& gpu_config_dto);

    StatusDtoT
    SetGpuConfig(const GPUConfigDtoT& gpu_config_dto);
#endif

    StatusDtoT
    CreateCollection(const milvus::server::web::OString& body);

    StatusDtoT
    ShowCollections(const OQueryParams& query_params, OString& result);

    StatusDtoT
    GetCollection(const OString& collection_name, const OQueryParams& query_params, OString& result);

    StatusDtoT
    DropCollection(const OString& collection_name);

    StatusDtoT
    CreateIndex(const OString& collection_name, const OString& field_name, const OString& body);

    StatusDtoT
    DropIndex(const OString& collection_name, const OString& field_name);

    StatusDtoT
    DeleteByIDs(const OString& collection_name, const OString& payload, OString& response);

    StatusDtoT
    CreatePartition(const OString& collection_name, const PartitionRequestDtoT& param);

    StatusDtoT
    ShowPartitions(const OString& collection_name, const OQueryParams& query_params,
                   PartitionListDtoT& partition_list_dto);

    StatusDtoT
    DropPartition(const OString& collection_name, const OString& body);

    /***********
     *
     * Segment
     */
    StatusDtoT
    ShowSegments(const OString& collection_name, const OQueryParams& query_params, OString& response);

    StatusDtoT
    GetSegmentInfo(const OString& collection_name, const OString& segment_name, const OString& info,
                   const OQueryParams& query_params, OString& result);

    /**
     *
     * Vector
     */
    StatusDtoT
    InsertEntity(const OString& collection_name, const OString& body, EntityIdsDtoT& ids_dto);

    Status
    GetEntity(const OString& collection_name, const OQueryParams& query_params, OString& response);

    StatusDtoT
    GetVector(const OString& collection_name, const OQueryParams& query_params, OString& response);

    StatusDtoT
    EntityOp(const OString& collection_name, const OQueryParams& query_params, const OString& payload,
             OString& response);

    /**
     *
     * System
     */
    StatusDtoT
    SystemInfo(const OString& cmd, const OQueryParams& query_params, OString& response_str);

    StatusDtoT
    SystemOp(const OString& op, const OString& body_str, OString& response_str);

    /**
     *
     * Status
     */
    StatusDtoT
    ServerStatus(OString& response_str);

 public:
    void
    RegisterRequestHandler(const ReqHandler& handler) {
        req_handler_ = handler;
    }

 private:
    std::shared_ptr<Context> context_ptr_;
    ReqHandler req_handler_;
    query::QueryPtr query_ptr_;
    std::unordered_map<std::string, engine::DataType> field_type_;
};

}  // namespace web
}  // namespace server
}  // namespace milvus
