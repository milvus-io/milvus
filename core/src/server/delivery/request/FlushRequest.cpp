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

#include "server/delivery/request/FlushRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>

namespace milvus {
namespace server {

FlushRequest::FlushRequest(const std::shared_ptr<Context>& context, const std::vector<std::string>& table_names)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP), table_names_(table_names) {
}

BaseRequestPtr
FlushRequest::Create(const std::shared_ptr<Context>& context, const std::vector<std::string>& table_names) {
    return std::shared_ptr<BaseRequest>(new FlushRequest(context, table_names));
}

Status
FlushRequest::OnExecute() {
    std::string hdr = "FlushRequest flush tables: ";
    for (auto& name : table_names_) {
        hdr += name;
        hdr += ", ";
    }

    TimeRecorderAuto rc(hdr);
    Status status = Status::OK();

    for (auto& name : table_names_) {
        // only process root table, ignore partition table
        engine::meta::TableSchema table_schema;
        table_schema.table_id_ = name;
        status = DBWrapper::DB()->DescribeTable(table_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(name));
            } else {
                return status;
            }
        } else {
            if (!table_schema.owner_table_.empty()) {
                return Status(SERVER_INVALID_TABLE_NAME, TableNotExistMsg(name));
            }
        }

        status = DBWrapper::DB()->Flush(name);
    }

    return status;
}

}  // namespace server
}  // namespace milvus
