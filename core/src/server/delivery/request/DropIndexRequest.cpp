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

#include "server/delivery/request/DropIndexRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>

namespace milvus {
namespace server {

DropIndexRequest::DropIndexRequest(const std::shared_ptr<Context>& context, const std::string& table_name)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP), table_name_(table_name) {
}

BaseRequestPtr
DropIndexRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name) {
    return std::shared_ptr<BaseRequest>(new DropIndexRequest(context, table_name));
}

Status
DropIndexRequest::OnExecute() {
    try {
        std::string hdr = "DropIndexRequest(table=" + table_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        bool has_table = false;
        status = DBWrapper::DB()->HasTable(table_name_, has_table);
        if (!status.ok()) {
            return status;
        }

        if (!has_table) {
            return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
        }

        // step 2: check table existence
        status = DBWrapper::DB()->DropIndex(table_name_);
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
