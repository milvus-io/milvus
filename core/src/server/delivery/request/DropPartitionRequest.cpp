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

#include "server/delivery/request/DropPartitionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>

namespace milvus {
namespace server {

DropPartitionRequest::DropPartitionRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                           const std::string& tag)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP), table_name_(table_name), tag_(tag) {
}

BaseRequestPtr
DropPartitionRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                             const std::string& tag) {
    return std::shared_ptr<BaseRequest>(new DropPartitionRequest(context, table_name, tag));
}

Status
DropPartitionRequest::OnExecute() {
    std::string hdr = "DropPartitionRequest(table=" + table_name_ + ", partition_tag=" + tag_ + ")";
    TimeRecorderAuto rc(hdr);

    std::string table_name = table_name_;
    std::string partition_tag = tag_;

    // step 1: check partition tag
    if (partition_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
        std::string msg = "Default partition cannot be dropped.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    auto status = ValidationUtil::ValidatePartitionTags({partition_tag});
    if (!status.ok()) {
        return status;
    }

    // step 2: check table
    bool exists;
    status = DBWrapper::DB()->HasTable(table_name, exists);
    if (!status.ok()) {
        return status;
    }

    if (!exists) {
        return Status(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
    }

    status = ValidationUtil::ValidateTableName(table_name);
    if (!status.ok()) {
        return status;
    }

    // step 3: drop partition
    return DBWrapper::DB()->DropPartitionByTag(table_name, partition_tag);
}

}  // namespace server
}  // namespace milvus
