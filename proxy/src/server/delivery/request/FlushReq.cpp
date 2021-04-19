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

#include "server/delivery/request/FlushReq.h"

#include <memory>
#include <unordered_map>
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace server {

FlushReq::FlushReq(const ContextPtr& context, const std::vector<std::string>& collection_names)
    : BaseReq(context, ReqType::kFlush), collection_names_(collection_names) {
}

BaseReqPtr
FlushReq::Create(const ContextPtr& context, const std::vector<std::string>& collection_names) {
    return std::shared_ptr<BaseReq>(new FlushReq(context, collection_names));
}

Status
FlushReq::OnExecute() {
    std::string hdr = "FlushReq flush collections: ";
    for (auto& name : collection_names_) {
        hdr += name;
        hdr += ", ";
    }

    TimeRecorderAuto rc(hdr);
    LOG_SERVER_DEBUG_ << hdr;


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
