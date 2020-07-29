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

#include "scheduler/job/SearchJob.h"
#include "scheduler/task/SearchTask.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

SearchJob::SearchJob(const server::ContextPtr& context, engine::DBOptions options, const query::QueryPtr& query_ptr)
    : Job(JobType::SEARCH), context_(context), options_(options), query_ptr_(query_ptr) {
    GetSegmentsFromQuery(query_ptr, segment_ids_);
}

void
SearchJob::OnCreateTasks(JobTasks& tasks) {
    for (auto& id : segment_ids_) {
        auto task = std::make_shared<SearchTask>(context_, options_, query_ptr_, id, nullptr);
        task->job_ = this;
        tasks.emplace_back(task);
    }
}

json
SearchJob::Dump() const {
    json ret{
        {"number_of_search_segment", segment_ids_.size()},
    };
    auto base = Job::Dump();
    ret.insert(base.begin(), base.end());
    return ret;
}

void
SearchJob::GetSegmentsFromQuery(const query::QueryPtr& query_ptr, engine::snapshot::IDS_TYPE& segment_ids) {
    // TODO
}

}  // namespace scheduler
}  // namespace milvus
