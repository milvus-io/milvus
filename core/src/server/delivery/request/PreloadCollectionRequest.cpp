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
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <unordered_map>
#include <vector>

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
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // only process root collection, ignore partition collection
        engine::snapshot::CollectionPtr collection;
        std::unordered_map<engine::snapshot::FieldPtr, std::vector<engine::snapshot::FieldElementPtr>> fields_schema;
        status = DBWrapper::SSDB()->DescribeCollection(collection_name_, collection, fields_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }

        // TODO(yukun): if PreloadCollection interface needs to add field names as params
        std::vector<std::string> field_names;
        for (auto field_it : fields_schema) {
            field_names.emplace_back(field_it.first->GetName());
        }

        // step 2: force load collection data into cache
        // load each segment and insert into cache even cache capacity is not enough
        status = DBWrapper::SSDB()->LoadCollection(context_, collection_name_, field_names, true);
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
