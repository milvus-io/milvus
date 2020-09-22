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

#include "server/delivery/request/CountEntitiesReq.h"
#include "BaseReq.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "server/MetaWrapper.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

CountEntitiesReq::CountEntitiesReq(const ContextPtr& context, const std::string& collection_name, int64_t& row_count)
    : BaseReq(context, ReqType::kCountEntities), collection_name_(collection_name), row_count_(row_count) {
}

BaseReqPtr
CountEntitiesReq::Create(const ContextPtr& context, const std::string& collection_name, int64_t& row_count) {
    return std::shared_ptr<BaseReq>(new CountEntitiesReq(context, collection_name, row_count));
}

Status
CountEntitiesReq::OnExecute() {
    try {
        std::string hdr = "CountEntitiesReq(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);
        row_count_ = MetaWrapper::GetInstance().CountCollection(collection_name_);

        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
