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

#include "server/grpc_impl/request/CreateTableRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>

namespace milvus {
namespace server {
namespace grpc {

CreateTableRequest::CreateTableRequest(const ::milvus::grpc::TableSchema* schema)
    : GrpcBaseRequest(DDL_DML_REQUEST_GROUP), schema_(schema) {
}

BaseRequestPtr
CreateTableRequest::Create(const ::milvus::grpc::TableSchema* schema) {
    if (schema == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseRequest>(new CreateTableRequest(schema));
}

Status
CreateTableRequest::OnExecute() {
    std::string hdr = "CreateTableRequest(table=" + schema_->table_name() +
                      ", dimension=" + std::to_string(schema_->dimension()) + ")";
    TimeRecorder rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(schema_->table_name());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableDimension(schema_->dimension());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexFileSize(schema_->index_file_size());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexMetricType(schema_->metric_type());
        if (!status.ok()) {
            return status;
        }

        // step 2: construct table schema
        engine::meta::TableSchema table_info;
        table_info.table_id_ = schema_->table_name();
        table_info.dimension_ = static_cast<uint16_t>(schema_->dimension());
        table_info.index_file_size_ = schema_->index_file_size();
        table_info.metric_type_ = schema_->metric_type();

        // step 3: create table
        status = DBWrapper::DB()->CreateTable(table_info);
        if (!status.ok()) {
            // table could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_TABLE_NAME, status.message());
            }
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
