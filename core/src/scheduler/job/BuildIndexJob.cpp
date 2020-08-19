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

namespace {
// each vector field create one group
// all structured fields put into one group
void
WhichFieldsToBuild(const engine::snapshot::ScopedSnapshotT& snapshot, engine::snapshot::ID_TYPE segment_id,
                   std::vector<engine::TargetFields>& field_groups) {
    auto field_names = snapshot->GetFieldNames();
    engine::TargetFields structured_fields;
    for (auto& field_name : field_names) {
        auto field = snapshot->GetField(field_name);
        engine::DataType ftype = static_cast<engine::DataType>(field->GetFtype());
        bool is_vector = (ftype == engine::DataType::VECTOR_FLOAT || ftype == engine::DataType::VECTOR_BINARY);
        auto elements = snapshot->GetFieldElementsByField(field_name);
        for (auto& element : elements) {
            if (element->GetFEtype() != engine::FieldElementType::FET_INDEX) {
                continue;  // only check index element
            }

            auto element_file = snapshot->GetSegmentFile(segment_id, element->GetID());
            if (element_file != nullptr) {
                continue;  // index file has been created, no need to build index for this field
            }

            // index has been defined, but index file not yet created, this field need to be build index
            if (is_vector) {
                engine::TargetFields fields = {field_name};
                field_groups.emplace_back(fields);
            } else {
                structured_fields.insert(field_name);
            }
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
}  // namespace scheduler
}  // namespace milvus
