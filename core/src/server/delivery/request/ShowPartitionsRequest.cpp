// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/delivery/request/ShowPartitionsRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <vector>

namespace milvus {
namespace server {

ShowPartitionsRequest::ShowPartitionsRequest(const std::shared_ptr<milvus::server::Context>& context,
                                             const std::string& collection_name,
                                             std::vector<PartitionParam>& partition_list)
    : BaseRequest(context, BaseRequest::kShowPartitions),
      collection_name_(collection_name),
      partition_list_(partition_list) {
}

BaseRequestPtr
ShowPartitionsRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                              const std::string& collection_name, std::vector<PartitionParam>& partition_list) {
    return std::shared_ptr<BaseRequest>(new ShowPartitionsRequest(context, collection_name, partition_list));
}

Status
ShowPartitionsRequest::OnExecute() {
    std::string hdr = "ShowPartitionsRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    // step 1: check collection name
    auto status = ValidationUtil::ValidateCollectionName(collection_name_);
    fiu_do_on("ShowPartitionsRequest.OnExecute.invalid_collection_name",
              status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        return status;
    }

    // step 2: check collection existence
    // only process root collection, ignore partition collection
    engine::meta::CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_name_;
    status = DBWrapper::DB()->DescribeCollection(collection_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
        } else {
            return status;
        }
    } else {
        if (!collection_schema.owner_collection_.empty()) {
            return Status(SERVER_INVALID_COLLECTION_NAME, CollectionNotExistMsg(collection_name_));
        }
    }

    // step 3: get partitions
    std::vector<engine::meta::CollectionSchema> schema_array;
    status = DBWrapper::DB()->ShowPartitions(collection_name_, schema_array);
    fiu_do_on("ShowPartitionsRequest.OnExecute.show_partition_fail",
              status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        return status;
    }

    partition_list_.clear();
    partition_list_.emplace_back(collection_name_, milvus::engine::DEFAULT_PARTITON_TAG);
    for (auto& schema : schema_array) {
        partition_list_.emplace_back(schema.owner_collection_, schema.partition_tag_);
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
