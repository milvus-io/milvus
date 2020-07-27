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

#include "server/delivery/request/DropCollectionReq.h"
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

DropCollectionRequest::DropCollectionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                             const std::string& collection_name)
    : BaseRequest(context, BaseRequest::kDropCollection), collection_name_(collection_name) {
}

BaseRequestPtr
DropCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                              const std::string& collection_name) {
    return std::shared_ptr<BaseRequest>(new DropCollectionRequest(context, collection_name));
}

Status
DropCollectionRequest::OnExecute() {
    try {
        std::string hdr = "DropCollectionRequest(collection=" + collection_name_ + ")";
        TimeRecorder rc(hdr);

        bool exist = false;
        auto status = DBWrapper::DB()->HasCollection(collection_name_, exist);
        if (!exist) {
            return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
        }

        STATUS_CHECK(DBWrapper::DB()->DropCollection(collection_name_));

        /* flush to trigger CleanUpFilesWithTTL */
        STATUS_CHECK(DBWrapper::DB()->Flush());

        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
