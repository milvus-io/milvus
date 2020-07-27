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

#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "server/delivery/request/GetCollectionStatsRequest.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

GetCollectionStatsRequest::GetCollectionStatsRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                     const std::string& collection_name, std::string& collection_stats)
    : BaseRequest(context, BaseRequest::kGetCollectionStats),
      collection_name_(collection_name),
      collection_stats_(collection_stats) {
}

BaseRequestPtr
GetCollectionStatsRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                  const std::string& collection_name, std::string& collection_stats) {
    return std::shared_ptr<BaseRequest>(new GetCollectionStatsRequest(context, collection_name, collection_stats));
}

Status
GetCollectionStatsRequest::OnExecute() {
    std::string hdr = "GetCollectionStatsRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    bool exist = false;
    auto status = DBWrapper::DB()->HasCollection(collection_name_, exist);
    if (!exist) {
        return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
    }

    STATUS_CHECK(DBWrapper::DB()->GetCollectionStats(collection_name_, collection_stats_));
    rc.ElapseFromBegin("done");

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
