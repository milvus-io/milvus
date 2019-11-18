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

#include "server/grpc_impl/request/ShowPartitionsRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {
namespace grpc {

ShowPartitionsRequest::ShowPartitionsRequest(const std::string& table_name,
                                             ::milvus::grpc::PartitionList* partition_list)
    : GrpcBaseRequest(INFO_REQUEST_GROUP), table_name_(table_name), partition_list_(partition_list) {
}

BaseRequestPtr
ShowPartitionsRequest::Create(const std::string& table_name, ::milvus::grpc::PartitionList* partition_list) {
    return std::shared_ptr<GrpcBaseRequest>(new ShowPartitionsRequest(table_name, partition_list));
}

Status
ShowPartitionsRequest::OnExecute() {
    auto status = ValidationUtil::ValidateTableName(table_name_);
    if (!status.ok()) {
        return status;
    }

    std::vector<engine::meta::TableSchema> schema_array;
    auto statuts = DBWrapper::DB()->ShowPartitions(table_name_, schema_array);
    if (!statuts.ok()) {
        return statuts;
    }

    for (auto& schema : schema_array) {
        ::milvus::grpc::PartitionParam* param = partition_list_->add_partition_array();
        param->set_table_name(schema.owner_table_);
        param->set_partition_name(schema.table_id_);
        param->set_tag(schema.partition_tag_);
    }
    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
