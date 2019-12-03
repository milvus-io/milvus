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

#include "server/grpc_impl/request/CreatePartitionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>

namespace milvus {
namespace server {
namespace grpc {

CreatePartitionRequest::CreatePartitionRequest(const ::milvus::grpc::PartitionParam* partition_param)
    : GrpcBaseRequest(DDL_DML_REQUEST_GROUP), partition_param_(partition_param) {
}

BaseRequestPtr
CreatePartitionRequest::Create(const ::milvus::grpc::PartitionParam* partition_param) {
    if (partition_param == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseRequest>(new CreatePartitionRequest(partition_param));
}

Status
CreatePartitionRequest::OnExecute() {
    std::string hdr = "CreatePartitionRequest(table=" + partition_param_->table_name() +
                      ", partition_name=" + partition_param_->partition_name() +
                      ", partition_tag=" + partition_param_->tag() + ")";
    TimeRecorderAuto rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(partition_param_->table_name());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidatePartitionName(partition_param_->partition_name());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidatePartitionTags({partition_param_->tag()});
        if (!status.ok()) {
            return status;
        }

        // step 2: create partition
        status = DBWrapper::DB()->CreatePartition(partition_param_->table_name(), partition_param_->partition_name(),
                                                  partition_param_->tag());
        if (!status.ok()) {
            // partition could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_TABLE_NAME, status.message());
            }
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
