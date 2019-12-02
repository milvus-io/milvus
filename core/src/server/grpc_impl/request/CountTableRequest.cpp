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

#include "server/grpc_impl/request/CountTableRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>

namespace milvus {
namespace server {
namespace grpc {

CountTableRequest::CountTableRequest(const std::string& table_name, int64_t& row_count)
    : GrpcBaseRequest(INFO_REQUEST_GROUP), table_name_(table_name), row_count_(row_count) {
}

BaseRequestPtr
CountTableRequest::Create(const std::string& table_name, int64_t& row_count) {
    return std::shared_ptr<GrpcBaseRequest>(new CountTableRequest(table_name, row_count));
}

Status
CountTableRequest::OnExecute() {
    try {
        std::string hdr = "CountTableRequest(table=" + table_name_ + ")";
        TimeRecorder rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: get row count
        uint64_t row_count = 0;
        status = DBWrapper::DB()->GetTableRowCount(table_name_, row_count);
        if (!status.ok()) {
            if (status.code(), DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        }

        row_count_ = static_cast<int64_t>(row_count);

        rc.ElapseFromBegin("total cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
