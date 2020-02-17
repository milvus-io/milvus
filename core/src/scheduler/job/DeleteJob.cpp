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

#include "scheduler/job/DeleteJob.h"

#include <utility>

namespace milvus {
namespace scheduler {

DeleteJob::DeleteJob(std::string table_id, engine::meta::MetaPtr meta_ptr, uint64_t num_resource)
    : Job(JobType::DELETE),
      table_id_(std::move(table_id)),
      meta_ptr_(std::move(meta_ptr)),
      num_resource_(num_resource) {
}

void
DeleteJob::WaitAndDelete() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return done_resource == num_resource_; });
    meta_ptr_->DeleteTableFiles(table_id_);
}

void
DeleteJob::ResourceDone() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++done_resource;
    }
    cv_.notify_one();
}

json
DeleteJob::Dump() const {
    json ret{
        {"table_id", table_id_},
        {"number_of_resource", num_resource_},
        {"number_of_done", done_resource},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

}  // namespace scheduler
}  // namespace milvus
