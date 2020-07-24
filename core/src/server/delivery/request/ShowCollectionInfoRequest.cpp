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

#include "server/delivery/request/ShowCollectionInfoRequest.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

ShowCollectionInfoRequest::ShowCollectionInfoRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                     const std::string& collection_name, std::string& collection_info)
    : BaseRequest(context, BaseRequest::kShowCollectionInfo),
      collection_name_(collection_name),
      collection_info_(collection_info) {
}

BaseRequestPtr
ShowCollectionInfoRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                  const std::string& collection_name, std::string& collection_info) {
    return std::shared_ptr<BaseRequest>(new ShowCollectionInfoRequest(context, collection_name, collection_info));
}

Status
ShowCollectionInfoRequest::OnExecute() {
    std::string hdr = "ShowCollectionInfoRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    // step 1: check collection name
    auto status = ValidateCollectionName(collection_name_);
    if (!status.ok()) {
        return status;
    }

    // step 2: check collection existence
    // only process root collection, ignore partition collection
    engine::snapshot::CollectionPtr collection;
    engine::snapshot::CollectionMappings fields_schema;
    status = DBWrapper::SSDB()->DescribeCollection(collection_name_, collection, fields_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
        } else {
            return status;
        }
    }

    // step 3: get partitions
    status = DBWrapper::SSDB()->GetCollectionInfo(collection_name_, collection_info_);
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
