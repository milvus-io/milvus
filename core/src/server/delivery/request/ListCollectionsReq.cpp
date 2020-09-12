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

#include <fiu/fiu-local.h>
#include <string>

namespace milvus {
namespace server {

ListCollectionsReq::ListCollectionsReq(const ContextPtr& context, std::vector<std::string>& collection_list)
    : BaseReq(context, ReqType::kListCollections), collection_list_(collection_list) {
}

BaseReqPtr
ListCollectionsReq::Create(const ContextPtr& context, std::vector<std::string>& collection_list) {
    return std::shared_ptr<BaseReq>(new ListCollectionsReq(context, collection_list));
}

Status
ListCollectionsReq::OnExecute() {
    TimeRecorderAuto rc("ListCollectionsReq");
    auto status = DBWrapper::DB()->ListCollections(collection_list_);
    fiu_do_on("ListCollectionsReq.OnExecute.show_collections_fail",
              status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        return status;
    }
    rc.ElapseFromBegin("done");
    return Status::OK();
}

}  // namespace server
}  // namespace milvus
