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

#include "server/delivery/request/DeleteByIDRequest.h"

#include <memory>
#include <string>
#include <vector>

#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace server {

DeleteByIDRequest::DeleteByIDRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                     const std::vector<int64_t>& vector_ids)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP), table_name_(table_name), vector_ids_(vector_ids) {
}

BaseRequestPtr
DeleteByIDRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                          const std::vector<int64_t>& vector_ids) {
    return std::shared_ptr<BaseRequest>(new DeleteByIDRequest(context, table_name, vector_ids));
}

Status
DeleteByIDRequest::OnExecute() {
    try {
        TimeRecorderAuto rc("DeleteByIDRequest");

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        engine::meta::TableSchema table_schema;
        table_schema.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        } else {
            if (!table_schema.owner_table_.empty()) {
                return Status(SERVER_INVALID_TABLE_NAME, TableNotExistMsg(table_name_));
            }
        }

        // Check table's index type supports delete
        if (table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IDMAP &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_BIN_IDMAP &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IVFFLAT &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_BIN_IVFFLAT &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IVFSQ8) {
            std::string err_msg =
                "Index type " + std::to_string(table_schema.engine_type_) + " does not support delete operation";
            SERVER_LOG_ERROR << err_msg;
            return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
        }

        rc.RecordSection("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_nq_" + this->delete_by_range_param_->table_name() + ".profiling";
        ProfilerStart(fname.c_str());
#endif
        status = DBWrapper::DB()->DeleteVectors(table_name_, vector_ids_);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
