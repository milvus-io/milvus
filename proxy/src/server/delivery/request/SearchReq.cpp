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

#include "server/delivery/request/SearchReq.h"
// #include "db/Utils.h"
#include "server/ValidationUtil.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#ifdef ENABLE_CPU_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

SearchReq::SearchReq(const ContextPtr& context, const query::QueryPtr& query_ptr, const milvus::json& json_params,
                      engine::QueryResultPtr& result)
    : BaseReq(context, ReqType::kSearch),
      query_ptr_(query_ptr),
      json_params_(json_params),
      result_(result) {
}

BaseReqPtr
SearchReq::Create(const ContextPtr& context, const query::QueryPtr& query_ptr, const milvus::json& json_params,
                  engine::QueryResultPtr& result) {
    return std::shared_ptr<BaseReq>(new SearchReq(context, query_ptr, json_params, result));
}

Status
SearchReq::OnExecute() {


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
