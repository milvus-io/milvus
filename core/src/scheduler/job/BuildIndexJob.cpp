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
#include "db/SnapshotUtils.h"
#include "db/Utils.h"
#include "scheduler/task/BuildIndexTask.h"
#include "utils/Log.h"

#include <utility>

namespace milvus {
namespace scheduler {

namespace {
// each vector field create one group
// all structured fields put into one group
void
WhichFieldsToBuild(const engine::snapshot::ScopedSnapshotT& snapshot, engine::snapshot::ID_TYPE segment_id,
                   std::vector<engine::TargetFields>& field_groups) {
    engine::TargetFields structured_fields;
    auto segment_visitor = engine::SegmentVisitor::Build(snapshot, segment_id);
    auto& field_visitors = segment_visitor->GetFieldVisitors();
    for (auto& pair : field_visitors) {
        auto& field_visitor = pair.second;
        if (!FieldRequireBuildIndex(field_visitor)) {
            continue;
        }

        // index has been defined, but index file not yet created, this field need to be build index
        auto& field = field_visitor->GetField();
        bool is_vector = engine::utils::IsVectorType(field->GetFtype());
        if (is_vector) {
            engine::TargetFields fields = {field->GetName()};
            field_groups.emplace_back(fields);
        } else {
            structured_fields.insert(field->GetName());
        }
    }

    if (!structured_fields.empty()) {
        field_groups.push_back(structured_fields);
    }
}

}  // namespace

BuildIndexJob::BuildIndexJob(const engine::snapshot::ScopedSnapshotT& snapshot, engine::DBOptions options,
                             const engine::snapshot::IDS_TYPE& segment_ids)
    : Job(JobType::BUILD), snapshot_(snapshot), options_(std::move(options)), segment_ids_(segment_ids) {
}

void
BuildIndexJob::OnCreateTasks(JobTasks& tasks) {
    for (auto& segment_id : segment_ids_) {
        std::vector<engine::TargetFields> field_groups;
        WhichFieldsToBuild(snapshot_, segment_id, field_groups);
        for (auto& group : field_groups) {
            auto task = std::make_shared<BuildIndexTask>(snapshot_, options_, segment_id, group, nullptr);
            task->job_ = this;
            tasks.emplace_back(task);
        }
    }
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

void
BuildIndexJob::MarkFailedSegment(engine::snapshot::ID_TYPE segment_id, const Status& status) {
    std::lock_guard<std::mutex> lock(failed_segments_mutex_);
    auto iter = failed_segments_.find(segment_id);
    if (iter == failed_segments_.end()) {
        failed_segments_.insert(std::make_pair(segment_id, status));
    } else {
        iter->second = status;
    }
}

SegmentFailedMap
BuildIndexJob::GetFailedSegments() {
    SegmentFailedMap result;
    {
        std::lock_guard<std::mutex> lock(failed_segments_mutex_);
        result = failed_segments_;
    }

    return result;
}

}  // namespace scheduler
}  // namespace milvus
