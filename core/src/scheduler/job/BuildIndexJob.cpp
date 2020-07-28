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
#include "scheduler/task/BuildIndexTask.h"

#include <utility>

#include "utils/Log.h"

namespace milvus {
namespace scheduler {

BuildIndexJob::BuildIndexJob(engine::DBOptions options, const std::string& collection_name,
                             const engine::snapshot::IDS_TYPE& segment_ids)
    : Job(JobType::BUILD), options_(std::move(options)), collection_name_(collection_name), segment_ids_(segment_ids) {
}

JobTasks
BuildIndexJob::CreateTasks() {
    std::vector<TaskPtr> tasks;
    for (auto& id : segment_ids_) {
        auto task = std::make_shared<BuildIndexTask>(options_, collection_name_, id, nullptr);
        task->job_ = this;
        tasks.emplace_back(task);
    }
    return tasks;
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
