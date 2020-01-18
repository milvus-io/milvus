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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include <opentracing/mocktracer/tracer.h>
#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include "server/web_impl/Types.h"
#include "server/web_impl/dto/CmdDto.hpp"
#include "server/web_impl/dto/ConfigDto.hpp"
#include "server/web_impl/dto/DevicesDto.hpp"
#include "server/web_impl/dto/IndexDto.hpp"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/dto/TableDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"

#include "db/Types.h"
#include "server/context/Context.h"
#include "server/delivery/RequestHandler.h"
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

 protected:
    Status
    GetTableInfo(const std::string& table_name, TableFieldsDto::ObjectWrapper& table_fields);

    Status
    CommandLine(const std::string& cmd, std::string& reply);

 public:
    WebRequestHandler() {
        context_ptr_ = GenContextPtr("Web Handler");
    }

    StatusDto::ObjectWrapper
    GetDevices(DevicesDto::ObjectWrapper& devices);

    StatusDto::ObjectWrapper
    GetAdvancedConfig(AdvancedConfigDto::ObjectWrapper& config);

    StatusDto::ObjectWrapper
    SetAdvancedConfig(const AdvancedConfigDto::ObjectWrapper& config);

#ifdef MILVUS_GPU_VERSION
    StatusDto::ObjectWrapper
    GetGpuConfig(GPUConfigDto::ObjectWrapper& gpu_config_dto);

    StatusDto::ObjectWrapper
    SetGpuConfig(const GPUConfigDto::ObjectWrapper& gpu_config_dto);
#endif

    StatusDto::ObjectWrapper
    CreateTable(const TableRequestDto::ObjectWrapper& table_schema);

    StatusDto::ObjectWrapper
    GetTable(const OString& table_name, const OQueryParams& query_params, TableFieldsDto::ObjectWrapper& schema_dto);

    StatusDto::ObjectWrapper
    ShowTables(const OString& offset, const OString& page_size, TableListFieldsDto::ObjectWrapper& table_list_dto);

    StatusDto::ObjectWrapper
    DropTable(const OString& table_name);

    StatusDto::ObjectWrapper
    CreateIndex(const OString& table_name, const IndexRequestDto::ObjectWrapper& index_param);

    StatusDto::ObjectWrapper
    GetIndex(const OString& table_name, IndexDto::ObjectWrapper& index_dto);

    StatusDto::ObjectWrapper
    DropIndex(const OString& table_name);

    StatusDto::ObjectWrapper
    CreatePartition(const OString& table_name, const PartitionRequestDto::ObjectWrapper& param);

    StatusDto::ObjectWrapper
    ShowPartitions(const OString& offset, const OString& page_size, const OString& table_name,
                   PartitionListDto::ObjectWrapper& partition_list_dto);

    StatusDto::ObjectWrapper
    DropPartition(const OString& table_name, const OString& tag);

    StatusDto::ObjectWrapper
    Insert(const OString& table_name, const InsertRequestDto::ObjectWrapper& param,
           VectorIdsDto::ObjectWrapper& ids_dto);

    StatusDto::ObjectWrapper
    Search(const OString& table_name, const SearchRequestDto::ObjectWrapper& search_request,
           TopkResultsDto::ObjectWrapper& results_dto);

    StatusDto::ObjectWrapper
    Cmd(const OString& cmd, CommandDto::ObjectWrapper& cmd_dto);

 public:
    WebRequestHandler&
    RegisterRequestHandler(const RequestHandler& handler) {
        request_handler_ = handler;
    }

 private:
    std::shared_ptr<Context> context_ptr_;
    RequestHandler request_handler_;
};

}  // namespace web
}  // namespace server
}  // namespace milvus
