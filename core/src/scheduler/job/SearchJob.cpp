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

#include "scheduler/job/SearchJob.h"

#include "utils/Log.h"

namespace milvus {
namespace scheduler {

SearchJob::SearchJob(const std::shared_ptr<server::Context>& context, uint64_t topk, uint64_t nq, uint64_t nprobe,
                     const float* vectors)
    : Job(JobType::SEARCH), context_(context), topk_(topk), nq_(nq), nprobe_(nprobe), vectors_(vectors) {
}

bool
SearchJob::AddIndexFile(const TableFileSchemaPtr& index_file) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (index_file == nullptr || index_files_.find(index_file->id_) != index_files_.end()) {
        return false;
    }

    SERVER_LOG_DEBUG << "SearchJob " << id() << " add index file: " << index_file->id_;

    index_files_[index_file->id_] = index_file;
    return true;
}

void
SearchJob::WaitResult() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return index_files_.empty(); });
    SERVER_LOG_DEBUG << "SearchJob " << id() << " all done";
}

void
SearchJob::SearchDone(size_t index_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    index_files_.erase(index_id);
    if (index_files_.empty()) {
        cv_.notify_all();
    }

    SERVER_LOG_DEBUG << "SearchJob " << id() << " finish index file: " << index_id;
}

ResultIds&
SearchJob::GetResultIds() {
    return result_ids_;
}

ResultDistances&
SearchJob::GetResultDistances() {
    return result_distances_;
}

Status&
SearchJob::GetStatus() {
    return status_;
}

json
SearchJob::Dump() const {
    json ret{
        {"topk", topk_},
        {"nq", nq_},
        {"nprobe", nprobe_},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

const std::shared_ptr<server::Context>&
SearchJob::GetContext() const {
    return context_;
}

}  // namespace scheduler
}  // namespace milvus
