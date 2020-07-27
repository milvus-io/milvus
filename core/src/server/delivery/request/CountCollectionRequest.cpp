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

#include "server/delivery/request/CountCollectionRequest.h"
#include "BaseRequest.h"
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

CountEntitiesRequest::CountEntitiesRequest(const std::shared_ptr<milvus::server::Context>& context,
                                           const std::string& collection_name, int64_t& row_count)
    : BaseRequest(context, BaseRequest::kCountEntities), collection_name_(collection_name), row_count_(row_count) {
}

BaseRequestPtr
CountEntitiesRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                             const std::string& collection_name, int64_t& row_count) {
    return std::shared_ptr<BaseRequest>(new CountEntitiesRequest(context, collection_name, row_count));
}

Status
CountEntitiesRequest::OnExecute() {
    try {
        std::string hdr = "CountEntitiesRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        bool exist = false;
        auto status = DBWrapper::DB()->HasCollection(collection_name_, exist);
        if (!exist) {
            return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
        }

        // step 2: get row count
        uint64_t row_count = 0;
        status = DBWrapper::DB()->GetCollectionRowCount(collection_name_, row_count);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }

        row_count_ = static_cast<int64_t>(row_count);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
