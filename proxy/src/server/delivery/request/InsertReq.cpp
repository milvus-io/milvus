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

#include "server/delivery/request/InsertReq.h"
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

InsertReq::InsertReq(const ContextPtr& context, const std::string& collection_name, const std::string& partition_name,
                     const int64_t& row_count, std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data)
    : BaseReq(context, ReqType::kInsert),
      collection_name_(collection_name),
      partition_name_(partition_name),
      row_count_(row_count),
      chunk_data_(chunk_data) {
}

BaseReqPtr
InsertReq::Create(const ContextPtr& context, const std::string& collection_name, const std::string& partition_name,
                  const int64_t& row_count, std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data) {
    return std::shared_ptr<BaseReq>(new InsertReq(context, collection_name, partition_name, row_count, chunk_data));
}

Status
InsertReq::OnExecute() {
    LOG_SERVER_INFO_ << LogOut("[%s][%ld] ", "insert", 0) << "Execute InsertReq.";


    return Status::OK();
}

}  // namespace server
}  // namespace milvus
