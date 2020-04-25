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

#include "server/delivery/request/HasPartitionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <vector>

namespace milvus {
namespace server {

HasPartitionRequest::HasPartitionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                         const std::string& collection_name, const std::string& tag,
                                         bool& has_partition)
    : BaseRequest(context, BaseRequest::kHasCollection),
      collection_name_(collection_name),
      partition_tag_(tag),
      has_partition_(has_partition) {
}

BaseRequestPtr
HasPartitionRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                            const std::string& tag, bool& has_partition) {
    return std::shared_ptr<BaseRequest>(new HasPartitionRequest(context, collection_name, tag, has_partition));
}

Status
HasPartitionRequest::OnExecute() {
    try {
        std::string hdr = "HasPartitionRequest(collection=" + collection_name_ + " tag=" + partition_tag_ + ")";
        TimeRecorderAuto rc(hdr);

        has_partition_ = false;

        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        std::vector<engine::meta::CollectionSchema> schema_array;
        status = DBWrapper::DB()->ShowPartitions(collection_name_, schema_array);
        if (!status.ok()) {
            return status;
        }

        for (auto& schema : schema_array) {
            if (schema.partition_tag_ == partition_tag_) {
                has_partition_ = true;
                break;
            }
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
