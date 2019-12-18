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
#include <oatpp/web/server/api/ApiController.hpp>

#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/core/macro/codegen.hpp>

#include "server/web_impl/Types.h"
#include "server/web_impl/dto/CmdDto.hpp"
#include "server/web_impl/dto/IndexDto.hpp"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/dto/TableDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"

#include "server/context/Context.h"
#include "server/delivery/RequestHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace server {
namespace web {

#define ASSIGN_STATUS_DTO(STATUS_DTO, STATUS)                      \
    do {                                                           \
        (STATUS_DTO)->code = static_cast<OInt64>((STATUS).code()); \
        (STATUS_DTO)->message = (STATUS).message().c_str();        \
    } while (false);

class WebHandler {
 private:
    std::shared_ptr<Context>
    MockContextPtr(const std::string& context_str) {
        auto context_ptr = std::make_shared<Context>("dummy_request_id");
        opentracing::mocktracer::MockTracerOptions tracer_options;
        auto mock_tracer =
            std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
        auto mock_span = mock_tracer->StartSpan("mock_span");
        auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
        context_ptr->SetTraceContext(trace_context);

        return context_ptr;
    }

 public:
    WebHandler() {
        context_ptr_ = MockContextPtr("Web Handler");
    }

    void
    CreateTable(const TableRequestDto::ObjectWrapper& table_schema, StatusDto::ObjectWrapper& status_dto);

    void
    GetTable(const OString& table_name, const OQueryParams& query_params, StatusDto::ObjectWrapper& status_dto,
             TableFieldsDto::ObjectWrapper& schema_dto);

    void
    ShowTables(const OInt64& offset, const OInt64& page_size, StatusDto::ObjectWrapper& status_dto,
               TableListDto::ObjectWrapper& table_list_dto);

    void
    DropTable(const OString& table_name, StatusDto::ObjectWrapper& status_dto);

    void
    CreateIndex(const OString& table_name, const IndexRequestDto::ObjectWrapper& index_param,
                StatusDto::ObjectWrapper& status_dto);

    void
    GetIndex(const OString& table_name, StatusDto::ObjectWrapper& status_dto, IndexDto::ObjectWrapper& index_dto);

    void
    DropIndex(const OString& table_name, StatusDto::ObjectWrapper& status_dto);

    void
    CreatePartition(const OString& table_name, const PartitionRequestDto::ObjectWrapper& param,
                    StatusDto::ObjectWrapper& status_dto);

    void
    ShowPartitions(const OInt64& offset, const OInt64& page_size, const OString& table_name,
                   StatusDto::ObjectWrapper& status_dto, PartitionListDto::ObjectWrapper& partition_list_dto);

    void
    DropPartition(const OString& table_name, const OString& tag, StatusDto::ObjectWrapper& status_dto);

    void
    Insert(const OQueryParams& query_params, const InsertRequestDto::ObjectWrapper& param,
           StatusDto::ObjectWrapper& status_dto, VectorIdsDto::ObjectWrapper& ids_dto);

    void
    Search(const OString& table_name, const OInt64& topk, const OInt64& nprobe, const OQueryParams& query_params,
           const RecordsDto::ObjectWrapper& records, StatusDto::ObjectWrapper& status_dto,
           ResultDto::ObjectWrapper& results_dto);

    void
    Cmd(const OString& cmd, StatusDto::ObjectWrapper& status_dto, CommandDto::ObjectWrapper& cmd_dto);

    WebHandler&
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
