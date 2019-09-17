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


#include "SearchContext.h"
#include "utils/Log.h"

#include <chrono>

namespace zilliz {
namespace milvus {
namespace engine {

SearchContext::SearchContext(uint64_t topk, uint64_t nq, uint64_t nprobe, const float* vectors)
    : IScheduleContext(ScheduleContextType::kSearch),
      topk_(topk),
      nq_(nq),
      nprobe_(nprobe),
      vectors_(vectors) {
    //use current time to identify this context
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
    long id = tp.time_since_epoch().count();
    identity_ = std::to_string(id);
}

bool
SearchContext::AddIndexFile(TableFileSchemaPtr& index_file) {
    std::unique_lock <std::mutex> lock(mtx_);
    if(index_file == nullptr || map_index_files_.find(index_file->id_) != map_index_files_.end()) {
        return false;
    }

    SERVER_LOG_DEBUG << "SearchContext " << identity_ << " add index file: " << index_file->id_;

    map_index_files_[index_file->id_] = index_file;
    return true;
}

void
SearchContext::IndexSearchDone(size_t index_id) {
    std::unique_lock <std::mutex> lock(mtx_);
    map_index_files_.erase(index_id);
    done_cond_.notify_all();
    SERVER_LOG_DEBUG << "SearchContext " << identity_ << " finish index file: " << index_id;
}

void
SearchContext::WaitResult() {
    std::unique_lock <std::mutex> lock(mtx_);
    done_cond_.wait(lock, [this] { return map_index_files_.empty(); });
    SERVER_LOG_DEBUG << "SearchContext " << identity_ << " all done";
}

}
}
}