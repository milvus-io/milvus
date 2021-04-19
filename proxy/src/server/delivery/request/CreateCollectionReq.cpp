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

#include "server/delivery/request/CreateCollectionReq.h"
// #include "db/Utils.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <set>

namespace milvus {
namespace server {

CreateCollectionReq::CreateCollectionReq(const ContextPtr& context, const std::string& collection_name,
                                         FieldsType& fields, milvus::json& extra_params)
    : BaseReq(context, ReqType::kCreateCollection),
      collection_name_(collection_name),
      fields_(fields),
      extra_params_(extra_params) {
}

BaseReqPtr
CreateCollectionReq::Create(const ContextPtr& context, const std::string& collection_name, FieldsType& fields,
                            milvus::json& extra_params) {
    return std::shared_ptr<BaseReq>(new CreateCollectionReq(context, collection_name, fields, extra_params));
}

Status
CreateCollectionReq::OnExecute() {


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
