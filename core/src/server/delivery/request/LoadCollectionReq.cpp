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

#include "server/delivery/request/LoadCollectionReq.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu/fiu-local.h>
#include <memory>
#include <vector>

namespace milvus {
namespace server {

LoadCollectionReq::LoadCollectionReq(const ContextPtr& context, const std::string& collection_name)
    : BaseReq(context, ReqType::kLoadCollection), collection_name_(collection_name) {
}

BaseReqPtr
LoadCollectionReq::Create(const ContextPtr& context, const std::string& collection_name) {
    return std::shared_ptr<BaseReq>(new LoadCollectionReq(context, collection_name));
}

Status
LoadCollectionReq::OnExecute() {
    try {
        std::string hdr = "LoadCollectionReq(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        engine::snapshot::CollectionPtr collection;
        engine::snapshot::FieldElementMappings fields_schema;
        auto status = DBWrapper::DB()->GetCollectionInfo(collection_name_, collection, fields_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, "Collection not exist: " + collection_name_);
            } else {
                return status;
            }
        }

        // TODO(yukun): if PreloadCollection interface needs to add field names as params
        std::vector<std::string> field_names;
        for (const auto& field_it : fields_schema) {
            field_names.emplace_back(field_it.first->GetName());
        }

        // step 2: force load collection data into cache
        // load each segment and insert into cache even cache capacity is not enough
        status = DBWrapper::DB()->LoadCollection(context_, collection_name_, field_names, true);
        fiu_do_on("LoadCollectionReq.OnExecute.preload_collection_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("LoadCollectionReq.OnExecute.throw_std_exception", throw std::exception());
        if (!status.ok()) {
            return status;
        }

        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
