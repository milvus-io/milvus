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

#include "server/delivery/request/CreatePartitionReq.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

constexpr uint64_t MAX_PARTITION_NUM = 4096;

CreatePartitionReq::CreatePartitionReq(const ContextPtr& context, const std::string& collection_name,
                                       const std::string& tag)
    : BaseReq(context, ReqType::kCreatePartition), collection_name_(collection_name), tag_(tag) {
}

BaseReqPtr
CreatePartitionReq::Create(const ContextPtr& context, const std::string& collection_name, const std::string& tag) {
    return std::shared_ptr<BaseReq>(new CreatePartitionReq(context, collection_name, tag));
}

Status
CreatePartitionReq::OnExecute() {


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
