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

#include "server/delivery/request/DropPartitionReq.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu/fiu-local.h>
#include <memory>
#include <string>

namespace milvus {
namespace server {

DropPartitionReq::DropPartitionReq(const ContextPtr& context, const std::string& collection_name,
                                   const std::string& tag)
    : BaseReq(context, ReqType::kDropPartition), collection_name_(collection_name), tag_(tag) {
}

BaseReqPtr
DropPartitionReq::Create(const ContextPtr& context, const std::string& collection_name, const std::string& tag) {
    return std::shared_ptr<BaseReq>(new DropPartitionReq(context, collection_name, tag));
}

Status
DropPartitionReq::OnExecute() {
    try {
        std::string hdr = "DropPartitionReq(collection=" + collection_name_ + ", partition=" + tag_ + ")";
        TimeRecorderAuto rc(hdr);

        STATUS_CHECK(ValidateCollectionName(collection_name_));
        STATUS_CHECK(ValidatePartitionTag(tag_, true));

        /* check partition tag */
        if (tag_ == milvus::engine::DEFAULT_PARTITON_TAG) {
            std::string msg = "Default partition cannot be dropped.";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_COLLECTION_NAME, msg);
        }

        /* check collection */
        bool exist = false;
        STATUS_CHECK(DBWrapper::DB()->HasCollection(collection_name_, exist));
        if (!exist) {
            return Status(SERVER_COLLECTION_NOT_EXIST, "Collection not exist: " + collection_name_);
        }

        rc.RecordSection("check validation");

        /* drop partition */
        STATUS_CHECK(DBWrapper::DB()->DropPartition(collection_name_, tag_));
        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
