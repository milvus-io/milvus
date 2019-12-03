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

#include "server/grpc_impl/request/DropPartitionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>

namespace milvus {
namespace server {
namespace grpc {

DropPartitionRequest::DropPartitionRequest(const ::milvus::grpc::PartitionParam* partition_param)
    : GrpcBaseRequest(DDL_DML_REQUEST_GROUP), partition_param_(partition_param) {
}

BaseRequestPtr
DropPartitionRequest::Create(const ::milvus::grpc::PartitionParam* partition_param) {
    return std::shared_ptr<GrpcBaseRequest>(new DropPartitionRequest(partition_param));
}

Status
DropPartitionRequest::OnExecute() {
    std::string hdr = "DropPartitionRequest(table=" + partition_param_->table_name() +
                      ", partition_name=" + partition_param_->partition_name() +
                      ", partition_tag=" + partition_param_->tag() + ")";
    TimeRecorderAuto rc(hdr);

    std::string table_name = partition_param_->table_name();
    std::string partition_name = partition_param_->partition_name();
    std::string partition_tag = partition_param_->tag();
    if (!partition_name.empty()) {
        auto status = ValidationUtil::ValidateTableName(partition_name);
        if (!status.ok()) {
            return status;
        }

        // check partition existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = partition_name;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST,
                              "Table " + table_name + "'s partition " + partition_name + " not found");
            } else {
                return status;
            }
        }

        return DBWrapper::DB()->DropPartition(partition_name);
    } else {
        auto status = ValidationUtil::ValidateTableName(table_name);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidatePartitionTags({partition_tag});
        if (!status.ok()) {
            return status;
        }
        return DBWrapper::DB()->DropPartitionByTag(table_name, partition_tag);
    }
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
