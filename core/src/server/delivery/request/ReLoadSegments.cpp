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

#include "server/delivery/request/ReLoadSegments.h"


namespace milvus {
namespace server {

ReLoadSegments::ReLoadSegments(const std::shared_ptr<milvus::server::Context> &context, const std::string &collection_name, const std::vector<std::string> &segment_names)
    : BaseRequest(context, BaseRequest::kPreloadCollection), collection_name_(collection_name), segment_names_(segment_names) {
}

BaseRequestPtr
ReLoadSegments::Create(const std::shared_ptr<milvus::server::Context> &context, const std::string &collection_name, const std::vector<std::string>& segment_names) {
    return std::shared_ptr<BaseRequest>(new ReLoadSegments(context, collection_name, segment_names));
}

Status
ReLoadSegments::OnExecute() {
    return Status::OK();
}

}  // namespace server
}  // namespace milvus
