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

#include "server/delivery/request/DropPartitionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <string>

namespace milvus {
namespace server {

DropPartitionRequest::DropPartitionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                           const std::string& collection_name, const std::string& tag)
    : BaseRequest(context, BaseRequest::kDropPartition), collection_name_(collection_name), tag_(tag) {
}

BaseRequestPtr
DropPartitionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                             const std::string& collection_name, const std::string& tag) {
    return std::shared_ptr<BaseRequest>(new DropPartitionRequest(context, collection_name, tag));
}

Status
DropPartitionRequest::OnExecute() {
    std::string hdr = "DropPartitionRequest(collection=" + collection_name_ + ", partition_tag=" + tag_ + ")";
    TimeRecorderAuto rc(hdr);

    std::string collection_name = collection_name_;
    std::string partition_tag = tag_;

    // step 1: check collection name
    auto status = ValidationUtil::ValidateCollectionName(collection_name);
    fiu_do_on("DropPartitionRequest.OnExecute.invalid_collection_name",
              status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        return status;
    }

    // step 2: check partition tag
    if (partition_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
        std::string msg = "Default partition cannot be dropped.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    status = ValidationUtil::ValidatePartitionTags({partition_tag});
    if (!status.ok()) {
        return status;
    }

    // step 3: check collection
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

    rc.RecordSection("check validation");

    // step 4: drop partition
    return DBWrapper::DB()->DropPartitionByTag(collection_name, partition_tag);
}

}  // namespace server
}  // namespace milvus
