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

#include "server/delivery/request/CreatePartitionRequest.h"
#include "config/Config.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

CreatePartitionRequest::CreatePartitionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                               const std::string& collection_name, const std::string& tag)
    : BaseRequest(context, BaseRequest::kCreatePartition), collection_name_(collection_name), tag_(tag) {
}

BaseRequestPtr
CreatePartitionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                               const std::string& collection_name, const std::string& tag) {
    return std::shared_ptr<BaseRequest>(new CreatePartitionRequest(context, collection_name, tag));
}

Status
CreatePartitionRequest::OnExecute() {
    std::string hdr = "CreatePartitionRequest(collection=" + collection_name_ + ", partition_tag=" + tag_ + ")";
    TimeRecorderAuto rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        fiu_do_on("CreatePartitionRequest.OnExecute.invalid_collection_name",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        if (tag_ == milvus::engine::DEFAULT_PARTITON_TAG) {
            return Status(SERVER_INVALID_PARTITION_TAG, "'_default' is built-in partition tag");
        }

        status = ValidationUtil::ValidatePartitionTags({tag_});
        fiu_do_on("CreatePartitionRequest.OnExecute.invalid_partition_name",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        // check partition total count
        int64_t max_partition_num = 4096;
        status = Config::GetInstance().GetEngineConfigMaxPartitionNum(max_partition_num);
        if (!status.ok()) {
            return status;
        }

        int64_t partition_count = 0;
        status = DBWrapper::DB()->CountPartitions(collection_name_, partition_count);
        if (partition_count >= max_partition_num) {
            return Status(SERVER_UNSUPPORTED_ERROR, "The number of partitions exceeds the upper limit(4096)");
        }

        rc.RecordSection("check validation");

        // step 2: create partition
        status = DBWrapper::DB()->CreatePartition(collection_name_, "", tag_);
        fiu_do_on("CreatePartitionRequest.OnExecute.db_already_exist", status = Status(milvus::DB_ALREADY_EXIST, ""));
        fiu_do_on("CreatePartitionRequest.OnExecute.create_partition_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("CreatePartitionRequest.OnExecute.throw_std_exception", throw std::exception());
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            }
            // partition could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_COLLECTION_NAME, status.message());
            }
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
