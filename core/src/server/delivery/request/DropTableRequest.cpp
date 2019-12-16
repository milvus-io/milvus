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

#include "server/delivery/request/DropTableRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

DropTableRequest::DropTableRequest(const std::shared_ptr<Context>& context, const std::string& table_name)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP), table_name_(table_name) {
}

BaseRequestPtr
DropTableRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name) {
    return std::shared_ptr<BaseRequest>(new DropTableRequest(context, table_name));
}

Status
DropTableRequest::OnExecute() {
    try {
        std::string hdr = "DropTableRequest(table=" + table_name_ + ")";
        TimeRecorder rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        }

        rc.RecordSection("check validation");

        // step 3: Drop table
        std::vector<DB_DATE> dates;
        status = DBWrapper::DB()->DropTable(table_name_, dates);
        if (!status.ok()) {
            return status;
        }

        rc.ElapseFromBegin("total cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
