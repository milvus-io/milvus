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
#include "server/MetaWrapper.h"

#include <set>

namespace milvus {
namespace server {

CreateCollectionReq::CreateCollectionReq(const ContextPtr &context, const ::milvus::grpc::Mapping *request)
    : BaseReq(context, ReqType::kCreateCollection),
      request_(request) {
}

BaseReqPtr
CreateCollectionReq::Create(const ContextPtr &context, const ::milvus::grpc::Mapping *request) {
    return std::shared_ptr<BaseReq>(new CreateCollectionReq(context, request));
}

Status
CreateCollectionReq::OnExecute() {
    auto status = MetaWrapper::GetInstance().MetaClient()->CreateCollection(*request_);
    if (status.ok()){
      status = MetaWrapper::GetInstance().SyncMeta();
    }
    return status;
}

}  // namespace server
}  // namespace milvus
