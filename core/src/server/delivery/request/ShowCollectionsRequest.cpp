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

#include "server/delivery/request/ShowCollectionsRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

ShowCollectionsRequest::ShowCollectionsRequest(const std::shared_ptr<milvus::server::Context>& context,
                                               std::vector<std::string>& collection_name_list)
    : BaseRequest(context, BaseRequest::kShowCollections), collection_name_list_(collection_name_list) {
}

BaseRequestPtr
ShowCollectionsRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                               std::vector<std::string>& collection_name_list) {
    return std::shared_ptr<BaseRequest>(new ShowCollectionsRequest(context, collection_name_list));
}

Status
ShowCollectionsRequest::OnExecute() {
    TimeRecorderAuto rc("ShowCollectionsRequest");

    std::vector<engine::meta::CollectionSchema> schema_array;
    auto status = DBWrapper::DB()->AllCollections(schema_array);
    fiu_do_on("ShowCollectionsRequest.OnExecute.show_collections_fail",
              status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        return status;
    }

    for (auto& schema : schema_array) {
        collection_name_list_.push_back(schema.collection_id_);
    }
    return Status::OK();
}

}  // namespace server
}  // namespace milvus
