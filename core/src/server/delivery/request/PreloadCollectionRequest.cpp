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

#include "server/delivery/request/PreloadCollectionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>

namespace milvus {
namespace server {

PreloadCollectionRequest::PreloadCollectionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                   const std::string& collection_name)
    : BaseRequest(context, BaseRequest::kPreloadCollection), collection_name_(collection_name) {
}

BaseRequestPtr
PreloadCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                 const std::string& collection_name) {
    return std::shared_ptr<BaseRequest>(new PreloadCollectionRequest(context, collection_name));
}

Status
PreloadCollectionRequest::OnExecute() {
    try {
        std::string hdr = "PreloadCollectionRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

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

        // step 2: force load collection data into cache
        // load each segment and insert into cache even cache capacity is not enough
        status = DBWrapper::DB()->PreloadCollection(context_, collection_name_, true);
        fiu_do_on("PreloadCollectionRequest.OnExecute.preload_collection_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("PreloadCollectionRequest.OnExecute.throw_std_exception", throw std::exception());
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
