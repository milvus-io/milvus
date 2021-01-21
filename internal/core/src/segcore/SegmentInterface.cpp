// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/SegmentInterface.h"
#include "query/generated/ExecPlanNodeVisitor.h"
namespace milvus::segcore {
class Naive;

void
SegmentInterface::FillTargetEntry(const query::Plan* plan, QueryResult& results) const {
    AssertInfo(plan, "empty plan");
    auto size = results.result_distances_.size();
    Assert(results.internal_seg_offsets_.size() == size);
    Assert(results.result_offsets_.size() == size);
    Assert(results.row_data_.size() == 0);

    std::vector<int64_t> target(size);
    if (plan->schema_.get_is_auto_id()) {
        // use row_id
        bulk_subscript(SystemFieldType::RowId, results.internal_seg_offsets_.data(), size, target.data());
    } else {
        auto key_offset_opt = get_schema().get_primary_key_offset();
        Assert(key_offset_opt.has_value());
        auto key_offset = key_offset_opt.value();
        bulk_subscript(key_offset, results.internal_seg_offsets_.data(), size, target.data());
    }

    for (int64_t i = 0; i < size; ++i) {
        auto row_id = target[i];
        std::vector<char> blob(sizeof(row_id));
        memcpy(blob.data(), &row_id, sizeof(row_id));
        results.row_data_.emplace_back(std::move(blob));
    }
}

QueryResult
SegmentInterface::Search(const query::Plan* plan,
                         const query::PlaceholderGroup** placeholder_groups,
                         const Timestamp* timestamps,
                         int64_t num_groups) const {
    Assert(num_groups == 1);
    query::ExecPlanNodeVisitor visitor(*this, timestamps[0], *placeholder_groups[0]);
    auto results = visitor.get_moved_result(*plan->plan_node_);
    return results;
}

}  // namespace milvus::segcore
