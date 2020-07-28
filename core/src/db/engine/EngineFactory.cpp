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

#include "db/engine/EngineFactory.h"
#include "db/engine/ExecutionEngineImpl.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Log.h"

#include <memory>
#include <set>
#include <vector>

namespace milvus {
namespace engine {

ExecutionEnginePtr
EngineFactory::Build(const std::string& dir_root, const std::string& collection_name, int64_t segment_id) {
    snapshot::ScopedSnapshotT ss;
    snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    auto seg_visitor = engine::SegmentVisitor::Build(ss, segment_id);

    ExecutionEnginePtr execution_engine_ptr = std::make_shared<ExecutionEngineImpl>(dir_root, seg_visitor);

    return execution_engine_ptr;
}

void
EngineFactory::GroupFieldsForIndex(const std::string& collection_name, TargetFieldGroups& field_groups) {
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << collection_name << " doesn't exist: " << status.message();
        return;
    }

    std::set<std::string> structured_fields;
    std::vector<std::string> field_names = ss->GetFieldNames();
    for (auto& field_name : field_names) {
        auto field = ss->GetField(field_name);
        auto ftype = field->GetFtype();
        if (ftype == meta::hybrid::DataType::VECTOR_FLOAT || ftype == meta::hybrid::DataType::VECTOR_BINARY) {
            std::set<std::string> index_field = {field_name};
            field_groups.emplace_back(index_field);
        } else {
            structured_fields.insert(field_name);
        }
    }

    if (!structured_fields.empty()) {
        field_groups.emplace_back(structured_fields);
    }
}

}  // namespace engine
}  // namespace milvus
