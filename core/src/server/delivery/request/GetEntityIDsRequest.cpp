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

#include "server/delivery/request/GetEntityIDsRequest.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

GetEntityIDsRequest::GetEntityIDsRequest(const std::shared_ptr<milvus::server::Context>& context,
                                         const std::string& collection_name, int64_t segment_id,
                                         std::vector<int64_t>& vector_ids)
    : BaseRequest(context, BaseRequest::kGetVectorIDs),
      collection_name_(collection_name),
      segment_id_(segment_id),
      vector_ids_(vector_ids) {
}

BaseRequestPtr
GetEntityIDsRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                            int64_t segment_id, std::vector<int64_t>& vector_ids) {
    return std::shared_ptr<BaseRequest>(new GetEntityIDsRequest(context, collection_name, segment_id, vector_ids));
}

Status
GetEntityIDsRequest::OnExecute() {
    try {
        std::string hdr =
            "GetVectorIDsRequest(collection=" + collection_name_ + " segment=" + std::to_string(segment_id_) + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        engine::snapshot::CollectionPtr collection;
        engine::snapshot::CollectionMappings mappings;
        status = DBWrapper::DB()->DescribeCollection(collection_name_, collection, mappings);
        if (collection == nullptr) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }

        // step 2: get vector data, now only support get one id
        vector_ids_.clear();
        return DBWrapper::DB()->GetEntityIDs(collection_name_, segment_id_, vector_ids_);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
