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

#include "server/delivery/request/GetVectorByIDRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

GetVectorByIDRequest::GetVectorByIDRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                           const std::vector<int64_t>& ids, engine::VectorsData& vectors)
    : BaseRequest(context, INFO_REQUEST_GROUP), table_name_(table_name), ids_(ids), vectors_(vectors) {
}

BaseRequestPtr
GetVectorByIDRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                             const std::vector<int64_t>& ids, engine::VectorsData& vectors) {
    return std::shared_ptr<BaseRequest>(new GetVectorByIDRequest(context, table_name, ids, vectors));
}

Status
GetVectorByIDRequest::OnExecute() {
    try {
        std::string hdr = "GetVectorByIDRequest(table=" + table_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        if (ids_.empty()) {
            return Status(SERVER_INVALID_ARGUMENT, "No vector id specified");
        }

        // only process root table, ignore partition table
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

        // step 2: get vector data, now only support get one id
        return DBWrapper::DB()->GetVectorByID(table_name_, ids_[0], vectors_);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
