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

#include "server/delivery/request/ReLoadSegmentsRequest.h"

#include <fiu-local.h>

#include "config/ServerConfig.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace server {

ReLoadSegmentsRequest::ReLoadSegmentsRequest(const std::shared_ptr<milvus::server::Context>& context,
                                             const std::string& collection_name,
                                             const std::vector<std::string>& segment_ids)
    : BaseRequest(context, BaseRequest::kReloadSegments), collection_name_(collection_name), segment_ids_(segment_ids) {
}

BaseRequestPtr
ReLoadSegmentsRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                              const std::string& collection_name, const std::vector<std::string>& segment_ids) {
    return std::shared_ptr<BaseRequest>(new ReLoadSegmentsRequest(context, collection_name, segment_ids));
}

Status
ReLoadSegmentsRequest::OnExecute() {
    bool cluster_enable = config.cluster.enable();
    auto cluster_role = config.cluster.role();

    if ((not cluster_enable) || cluster_role == ClusterRole::RW) {
        // TODO: No need to reload segment files
        return Status(SERVER_SUCCESS, "");
    }

    try {
        std::string hdr = "ReloadSegmentsRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        std::vector<int64_t> segment_ids;
        for (auto& id : segment_ids_) {
            std::string::size_type sz;
            segment_ids.push_back(std::stoul(id, &sz));
        }

        return DBWrapper::DB()->ReLoadSegmentsDeletedDocs(collection_name_, segment_ids);
    } catch (std::exception& exp) {
        return Status(SERVER_UNEXPECTED_ERROR, exp.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
