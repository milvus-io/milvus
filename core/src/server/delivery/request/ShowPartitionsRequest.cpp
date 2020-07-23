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

#include "server/delivery/request/ShowPartitionsRequest.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <vector>

namespace milvus {
namespace server {

ShowPartitionsRequest::ShowPartitionsRequest(const std::shared_ptr<milvus::server::Context>& context,
                                             const std::string& collection_name,
                                             std::vector<std::string>& partition_list)
    : BaseRequest(context, BaseRequest::kShowPartitions),
      collection_name_(collection_name),
      partition_list_(partition_list) {
}

BaseRequestPtr
ShowPartitionsRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                              const std::string& collection_name, std::vector<std::string>& partition_list) {
    return std::shared_ptr<BaseRequest>(new ShowPartitionsRequest(context, collection_name, partition_list));
}

Status
ShowPartitionsRequest::OnExecute() {
    std::string hdr = "ShowPartitionsRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    /* check collection existence */
    bool exist = false;
    auto status = DBWrapper::DB()->HasCollection(collection_name_, exist);
    if (!exist) {
        return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
    }

    /* get partitions */
    status = DBWrapper::SSDB()->ShowPartitions(collection_name_, partition_list_);
    fiu_do_on("ShowPartitionsRequest.OnExecute.show_partition_fail",
              status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    return status;
}

}  // namespace server
}  // namespace milvus
