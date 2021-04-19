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

#include "server/delivery/request/ListPartitionsReq.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

ListPartitionsReq::ListPartitionsReq(const ContextPtr& context, const std::string& collection_name,
                                     std::vector<std::string>& partition_list)
    : BaseReq(context, ReqType::kListPartitions), collection_name_(collection_name), partition_list_(partition_list) {
}

BaseReqPtr
ListPartitionsReq::Create(const ContextPtr& context, const std::string& collection_name,
                          std::vector<std::string>& partition_list) {
    return std::shared_ptr<BaseReq>(new ListPartitionsReq(context, collection_name, partition_list));
}

Status
ListPartitionsReq::OnExecute() {
    std::string hdr = "ListPartitionsReq(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
