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

#include "server/delivery/request/CreateIndexReq.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

CreateIndexReq::CreateIndexReq(const ContextPtr& context, const std::string& collection_name,
                               const std::string& field_name, const std::string& index_name,
                               const milvus::json& json_params)
    : BaseReq(context, ReqType::kCreateIndex),
      collection_name_(collection_name),
      field_name_(field_name),
      index_name_(index_name),
      json_params_(json_params) {
}

BaseReqPtr
CreateIndexReq::Create(const ContextPtr& context, const std::string& collection_name, const std::string& field_name,
                       const std::string& index_name, const milvus::json& json_params) {
    return std::shared_ptr<BaseReq>(new CreateIndexReq(context, collection_name, field_name, index_name, json_params));
}

Status
CreateIndexReq::OnExecute() {

    return Status::OK();
}


}  // namespace server
}  // namespace milvus
