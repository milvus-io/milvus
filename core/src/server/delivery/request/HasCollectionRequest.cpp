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

#include "server/delivery/request/HasCollectionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>

namespace milvus {
namespace server {

HasCollectionRequest::HasCollectionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                           const std::string& collection_name, bool& has_collection)
    : BaseRequest(context, BaseRequest::kHasCollection),
      collection_name_(collection_name),
      has_collection_(has_collection) {
}

BaseRequestPtr
HasCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                             const std::string& collection_name, bool& has_collection) {
    return std::shared_ptr<BaseRequest>(new HasCollectionRequest(context, collection_name, has_collection));
}

Status
HasCollectionRequest::OnExecute() {
    try {
        std::string hdr = "HasCollectionRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check collection existence
        status = DBWrapper::DB()->HasNativeCollection(collection_name_, has_collection_);
        fiu_do_on("HasCollectionRequest.OnExecute.throw_std_exception", throw std::exception());

        return status;
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }
}

}  // namespace server
}  // namespace milvus
