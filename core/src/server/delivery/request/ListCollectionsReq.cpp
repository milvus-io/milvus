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

#include "server/delivery/request/ListCollectionsReq.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

ListCollectionsRequest::ListCollectionsRequest(const std::shared_ptr<milvus::server::Context>& context,
                                               std::vector<std::string>& collection_list)
    : BaseRequest(context, BaseRequest::kListCollections), collection_list_(collection_list) {
}

BaseRequestPtr
ListCollectionsRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                               std::vector<std::string>& collection_name_list) {
    return std::shared_ptr<BaseRequest>(new ListCollectionsRequest(context, collection_name_list));
}

Status
ListCollectionsRequest::OnExecute() {
    TimeRecorderAuto rc("ListCollectionsRequest");

    std::vector<std::string> names;
    auto status = DBWrapper::DB()->ListCollections(names);
    fiu_do_on("ListCollectionsRequest.OnExecute.show_collections_fail",
              status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        return status;
    }

    for (auto& name : names) {
        collection_list_.push_back(name);
    }
    return Status::OK();
}

}  // namespace server
}  // namespace milvus
