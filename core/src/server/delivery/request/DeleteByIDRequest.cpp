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

#include "server/delivery/request/DeleteByIDRequest.h"

#include <memory>
#include <string>
#include <vector>

#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace server {

DeleteByIDRequest::DeleteByIDRequest(const std::shared_ptr<milvus::server::Context>& context,
                                     const std::string& collection_name, const std::string& partition_tag,
                                     const std::vector<int64_t>& vector_ids)
    : BaseRequest(context, BaseRequest::kDeleteByID),
      collection_name_(collection_name),
      partition_tag_(partition_tag),
      vector_ids_(vector_ids) {
}

BaseRequestPtr
DeleteByIDRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                          const std::string& partition_tag, const std::vector<int64_t>& vector_ids) {
    return std::shared_ptr<BaseRequest>(new DeleteByIDRequest(context, collection_name, partition_tag, vector_ids));
}

Status
DeleteByIDRequest::OnExecute() {
    try {
        TimeRecorderAuto rc("DeleteByIDRequest");

        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        if (!partition_tag_.empty()) {
            status = ValidationUtil::ValidatePartitionTags({partition_tag_});
            if (!status.ok()) {
                return status;
            }
        }

        // step 2: check collection and partition existence
        bool has_or_not;
        DBWrapper::DB()->HasNativeCollection(collection_name_, has_or_not);
        if (!has_or_not) {
            return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
        }

        if (!partition_tag_.empty()) {
            DBWrapper::DB()->HasPartition(collection_name_, partition_tag_, has_or_not);
            if (!has_or_not) {
                return Status(SERVER_INVALID_PARTITION_TAG, PartitionNotExistMsg(collection_name_, partition_tag_));
            }
        }

        rc.RecordSection("check validation");

        status = DBWrapper::DB()->DeleteVectors(collection_name_, partition_tag_, vector_ids_);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
