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

BuildIndexJob::BuildIndexJob(engine::DBOptions options, const std::string& collection_name,
                             const engine::snapshot::IDS_TYPE& segment_ids)
    : Job(JobType::SS_BUILD),
      options_(std::move(options)),
      collection_name_(collection_name),
      segment_ids_(segment_ids) {
}

void
BuildIndexJob::WaitFinish() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return segment_ids_.empty(); });
    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] BuildIndexJob %ld all done", "build index", 0, id());
}

void
BuildIndexJob::BuildIndexDone(const engine::snapshot::ID_TYPE seg_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    for (engine::snapshot::IDS_TYPE::iterator iter = segment_ids_.begin(); iter != segment_ids_.end(); ++iter) {
        if (*iter == seg_id) {
            segment_ids_.erase(iter);
            break;
        }
    }
    if (segment_ids_.empty()) {
        cv_.notify_all();
    }
    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] BuildIndexJob %ld finish segment: %ld", "build index", 0, id(), seg_id);
}

json
BuildIndexJob::Dump() const {
    json ret{
        {"number_of_to_index_segment", segment_ids_.size()},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

}  // namespace scheduler
}  // namespace milvus
