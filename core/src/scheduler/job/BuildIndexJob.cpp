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

#include "scheduler/job/BuildIndexJob.h"

#include <utility>

#include "utils/Log.h"

namespace milvus {
namespace scheduler {

BuildIndexJob::BuildIndexJob(engine::meta::MetaPtr meta_ptr, engine::DBOptions options)
    : Job(JobType::BUILD), meta_ptr_(std::move(meta_ptr)), options_(std::move(options)) {
    SetIdentity("BuildIndexJob");
    AddCacheInsertDataListener();
}

bool
BuildIndexJob::AddToIndexFiles(const engine::meta::SegmentSchemaPtr& to_index_file) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (to_index_file == nullptr || to_index_files_.find(to_index_file->id_) != to_index_files_.end()) {
        return false;
    }

    LOG_SERVER_DEBUG_ << "BuildIndexJob " << id() << " add to_index file: " << to_index_file->id_
                      << ", location: " << to_index_file->location_;

    to_index_files_[to_index_file->id_] = to_index_file;
    return true;
}

void
BuildIndexJob::WaitBuildIndexFinish() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return to_index_files_.empty(); });
    LOG_SERVER_DEBUG_ << "BuildIndexJob " << id() << " all done";
}

void
BuildIndexJob::BuildIndexDone(size_t to_index_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    to_index_files_.erase(to_index_id);
    cv_.notify_all();
    LOG_SERVER_DEBUG_ << "BuildIndexJob " << id() << " finish index file: " << to_index_id;
}

json
BuildIndexJob::Dump() const {
    json ret{
        {"number_of_to_index_file", to_index_files_.size()},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

void
BuildIndexJob::OnCacheInsertDataChanged(bool value) {
    options_.insert_cache_immediately_ = value;
}

}  // namespace scheduler
}  // namespace milvus
