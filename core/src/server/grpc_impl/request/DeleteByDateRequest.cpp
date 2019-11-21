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

#include "server/grpc_impl/request/DeleteByDateRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {
namespace grpc {

DeleteByDateRequest::DeleteByDateRequest(const ::milvus::grpc::DeleteByDateParam* delete_by_range_param)
    : GrpcBaseRequest(DDL_DML_REQUEST_GROUP), delete_by_range_param_(delete_by_range_param) {
}

BaseRequestPtr
DeleteByDateRequest::Create(const ::milvus::grpc::DeleteByDateParam* delete_by_range_param) {
    if (delete_by_range_param == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }

    return std::shared_ptr<GrpcBaseRequest>(new DeleteByDateRequest(delete_by_range_param));
}

Status
DeleteByDateRequest::OnExecute() {
    try {
        TimeRecorder rc("DeleteByDateRequest");

        // step 1: check arguments
        std::string table_name = delete_by_range_param_->table_name();
        auto status = ValidationUtil::ValidateTableName(table_name);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code(), DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name));
            } else {
                return status;
            }
        }

        rc.ElapseFromBegin("check validation");

        // step 3: check date range, and convert to db dates
        std::vector<DB_DATE> dates;
        ErrorCode error_code = SERVER_SUCCESS;
        std::string error_msg;

        std::vector<::milvus::grpc::Range> range_array;
        range_array.emplace_back(delete_by_range_param_->range());
        status = ConvertTimeRangeToDBDates(range_array, dates);
        if (!status.ok()) {
            return status;
        }

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_nq_" + this->delete_by_range_param_->table_name() + ".profiling";
        ProfilerStart(fname.c_str());
#endif
        status = DBWrapper::DB()->DropTable(table_name, dates);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
