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

#include "server/delivery/request/HasPartitionReq.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

HasPartitionReq::HasPartitionReq(const ContextPtr& context, const std::string& collection_name, const std::string& tag,
                                 bool& has_partition)
    : BaseReq(context, ReqType::kHasCollection),
      collection_name_(collection_name),
      partition_tag_(tag),
      has_partition_(has_partition) {
}

BaseReqPtr
HasPartitionReq::Create(const ContextPtr& context, const std::string& collection_name, const std::string& tag,
                        bool& has_partition) {
    return std::shared_ptr<BaseReq>(new HasPartitionReq(context, collection_name, tag, has_partition));
}

Status
HasPartitionReq::OnExecute() {
    try {
        std::string hdr = "HasPartitionReq(collection=" + collection_name_ + " tag=" + partition_tag_ + ")";
        TimeRecorderAuto rc(hdr);

        has_partition_ = false;


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
