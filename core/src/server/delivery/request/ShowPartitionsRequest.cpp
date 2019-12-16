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

#include "server/delivery/request/ShowPartitionsRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

ShowPartitionsRequest::ShowPartitionsRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                             std::vector<PartitionParam>& partition_list)
    : BaseRequest(context, INFO_REQUEST_GROUP), table_name_(table_name), partition_list_(partition_list) {
}

BaseRequestPtr
ShowPartitionsRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                              std::vector<PartitionParam>& partition_list) {
    return std::shared_ptr<BaseRequest>(new ShowPartitionsRequest(context, table_name, partition_list));
}

Status
ShowPartitionsRequest::OnExecute() {
    std::string hdr = "ShowPartitionsRequest(table=" + table_name_ + ")";
    TimeRecorderAuto rc(hdr);

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
        partition_list_.emplace_back(schema.owner_table_, schema.table_id_, schema.partition_tag_);
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
