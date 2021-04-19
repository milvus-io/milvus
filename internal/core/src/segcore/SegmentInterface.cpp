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
SegmentInternalInterface::FillTargetEntry(const query::Plan* plan, QueryResult& results) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.result_distances_.size();
    Assert(results.internal_seg_offsets_.size() == size);
    Assert(results.result_offsets_.size() == size);
    Assert(results.row_data_.size() == 0);

    // std::vector<int64_t> row_ids(size);
    std::vector<int64_t> element_sizeofs;
    std::vector<aligned_vector<char>> blobs;

    // fill row_ids
    {
        aligned_vector<char> blob(size * sizeof(int64_t));
        if (plan->schema_.get_is_auto_id()) {
            bulk_subscript(SystemFieldType::RowId, results.internal_seg_offsets_.data(), size, blob.data());
        } else {
            auto key_offset_opt = get_schema().get_primary_key_offset();
            Assert(key_offset_opt.has_value());
            auto key_offset = key_offset_opt.value();
            Assert(get_schema()[key_offset].get_data_type() == DataType::INT64);
            bulk_subscript(key_offset, results.internal_seg_offsets_.data(), size, blob.data());
        }
        blobs.emplace_back(std::move(blob));
        element_sizeofs.push_back(sizeof(int64_t));
    }

    // fill other entries
    for (auto field_offset : plan->target_entries_) {
        auto& field_meta = get_schema()[field_offset];
        auto element_sizeof = field_meta.get_sizeof();
        aligned_vector<char> blob(size * element_sizeof);
        bulk_subscript(field_offset, results.internal_seg_offsets_.data(), size, blob.data());
        blobs.emplace_back(std::move(blob));
        element_sizeofs.push_back(element_sizeof);
    }

    auto target_sizeof = std::accumulate(element_sizeofs.begin(), element_sizeofs.end(), 0);

    for (int64_t i = 0; i < size; ++i) {
        int64_t element_offset = 0;
        std::vector<char> target(target_sizeof);
        for (int loc = 0; loc < blobs.size(); ++loc) {
            auto element_sizeof = element_sizeofs[loc];
            auto blob_ptr = blobs[loc].data();
            auto src = blob_ptr + element_sizeof * i;
            auto dst = target.data() + element_offset;
            memcpy(dst, src, element_sizeof);
            element_offset += element_sizeof;
        }
        assert(element_offset == target_sizeof);
        results.row_data_.emplace_back(std::move(target));
    }
}

QueryResult
SegmentInternalInterface::Search(const query::Plan* plan,
                                 const query::PlaceholderGroup** placeholder_groups,
                                 const Timestamp* timestamps,
                                 int64_t num_groups) const {
    std::shared_lock lck(mutex_);
    check_search(plan);
    Assert(num_groups == 1);
    query::ExecPlanNodeVisitor visitor(*this, timestamps[0], *placeholder_groups[0]);
    auto results = visitor.get_moved_result(*plan->plan_node_);
    return results;
}

}  // namespace milvus::segcore
