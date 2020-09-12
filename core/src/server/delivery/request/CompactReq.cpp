// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "server/delivery/request/CompactReq.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

CompactReq::CompactReq(const ContextPtr& context, const std::string& collection_name, double compact_threshold)
    : BaseReq(context, ReqType::kCompact), collection_name_(collection_name), compact_threshold_(compact_threshold) {
}

BaseReqPtr
CompactReq::Create(const ContextPtr& context, const std::string& collection_name, double compact_threshold) {
    return std::shared_ptr<BaseReq>(new CompactReq(context, collection_name, compact_threshold));
}

Status
CompactReq::OnExecute() {
    try {
        std::string hdr = "CompactReq(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        auto status = ValidateCompactThreshold(compact_threshold_);
        if (!status.ok()) {
            return status;
        }

        bool exist = false;
        STATUS_CHECK(DBWrapper::DB()->HasCollection(collection_name_, exist));
        if (!exist) {
            return Status(SERVER_COLLECTION_NOT_EXIST, "Collection not exist: " + collection_name_);
        }

        STATUS_CHECK(DBWrapper::DB()->Compact(context_, collection_name_, compact_threshold_));
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
