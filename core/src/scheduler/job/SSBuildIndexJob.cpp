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

#include "scheduler/job/SSBuildIndexJob.h"

#include <utility>

#include "utils/Log.h"

namespace milvus {
namespace scheduler {

SSBuildIndexJob::SSBuildIndexJob(engine::DBOptions options) : Job(JobType::SS_BUILD), options_(std::move(options)) {
    SetIdentity("SSBuildIndexJob");
    AddCacheInsertDataListener();
}

void
SSBuildIndexJob::AddSegmentVisitor(const engine::SegmentVisitorPtr& visitor) {
    if (visitor != nullptr) {
        segment_visitor_map_[visitor->GetSegment()->GetID()] = visitor;
    }
}

void
SSBuildIndexJob::WaitBuildIndexFinish() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return segment_visitor_map_.empty(); });
    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] BuildIndexJob %ld all done", "build index", 0, id());
}

void
SSBuildIndexJob::BuildIndexDone(const engine::snapshot::ID_TYPE seg_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    segment_visitor_map_.erase(seg_id);
    cv_.notify_all();
    LOG_SERVER_DEBUG_ << LogOut("[%s][%ld] BuildIndexJob %ld finish segment: %ld", "build index", 0, id(), seg_id);
}

json
SSBuildIndexJob::Dump() const {
    json ret{
        {"number_of_to_index_file", segment_visitor_map_.size()},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

void
SSBuildIndexJob::OnCacheInsertDataChanged(bool value) {
    options_.insert_cache_immediately_ = value;
}

}  // namespace scheduler
}  // namespace milvus
