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
    if (!partition_param_->partition_name().empty()) {
        auto status = ValidationUtil::ValidateTableName(partition_param_->partition_name());
        if (!status.ok()) {
            return status;
        }
        return DBWrapper::DB()->DropPartition(partition_param_->partition_name());
    } else {
        auto status = ValidationUtil::ValidateTableName(partition_param_->table_name());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidatePartitionTags({partition_param_->tag()});
        if (!status.ok()) {
            return status;
        }
        return DBWrapper::DB()->DropPartitionByTag(partition_param_->table_name(), partition_param_->tag());
    }
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
