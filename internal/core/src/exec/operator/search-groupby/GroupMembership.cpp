// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/operator/search-groupby/GroupMembership.h"

#include <algorithm>
#include <memory>
#include <unordered_map>

#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"

namespace milvus::exec {
namespace {

template <typename T>
using GroupKey = std::optional<T>;

bool
IsEligible(const TargetBitmap* base_filter, size_t offset) {
    return base_filter == nullptr || !(*base_filter)[offset];
}

template <typename T, typename Visitor>
bool
ScanRawField(milvus::OpContext* op_ctx,
             const segcore::SegmentInternalInterface& segment,
             FieldId field_id,
             size_t row_count,
             Visitor&& visitor) {
    segcore::CheckCancellation(op_ctx,
                               segment.get_segment_id(),
                               field_id.get(),
                               "strict group membership");
    if (!segment.HasFieldData(field_id)) {
        return false;
    }
    if (row_count == 0) {
        return true;
    }
    if (auto growing =
            dynamic_cast<const segcore::SegmentGrowingImpl*>(&segment)) {
        auto values = growing->get_insert_record().get_data<T>(field_id);
        auto valid = growing->get_insert_record().is_valid_data_exist(field_id)
                         ? growing->get_insert_record().get_valid_data(field_id)
                         : nullptr;
        for (size_t offset = 0; offset < row_count; ++offset) {
            if ((offset & 1023) == 0) {
                segcore::CheckCancellation(op_ctx,
                                           segment.get_segment_id(),
                                           field_id.get(),
                                           "strict group membership");
            }
            if (valid && !valid->is_valid(offset)) {
                visitor(offset, GroupKey<T>(std::nullopt));
                continue;
            }
            if constexpr (std::is_same_v<T, std::string>) {
                if (values->is_mmap()) {
                    visitor(
                        offset,
                        GroupKey<T>(std::string(values->view_element(offset))));
                    continue;
                }
            }
            visitor(offset, GroupKey<T>(static_cast<T>((*values)[offset])));
        }
        return true;
    }
    auto raw_chunk_count = segment.num_chunk_data(field_id);
    if (raw_chunk_count == 0 ||
        segment.num_rows_until_chunk(field_id, 0) != 0) {
        return false;
    }
    size_t raw_row_count = 0;
    for (int64_t chunk = 0; chunk < raw_chunk_count; ++chunk) {
        raw_row_count += segment.chunk_size(field_id, chunk);
    }
    if (raw_row_count < row_count) {
        // A partially indexed field may only retain raw data for a suffix of
        // the segment. Do not reinterpret that suffix as logical offset zero.
        return false;
    }

    int64_t chunk_id = 0;
    int64_t chunk_pos = 0;
    segcore::SegmentChunkReader reader(op_ctx, &segment, row_count);
    auto accessor =
        reader.GetMultipleChunkDataAccessor(segment.GetFieldDataType(field_id),
                                            field_id,
                                            chunk_id,
                                            chunk_pos,
                                            segcore::PinnedIndexView{});
    for (size_t offset = 0; offset < row_count; ++offset) {
        if ((offset & 1023) == 0) {
            segcore::CheckCancellation(op_ctx,
                                       segment.get_segment_id(),
                                       field_id.get(),
                                       "strict group membership");
        }
        auto value = accessor();
        if (value.has_value()) {
            visitor(offset, GroupKey<T>(segcore::get_from_variant<T>(value)));
        } else {
            visitor(offset, GroupKey<T>(std::nullopt));
        }
    }
    return true;
}

}  // namespace

template <typename T>
std::optional<std::vector<std::vector<int64_t>>>
BuildGroupOffsets(milvus::OpContext* op_ctx,
                  const segcore::SegmentInternalInterface& segment,
                  FieldId field_id,
                  int64_t row_count,
                  const std::vector<GroupKey<T>>& groups,
                  const TargetBitmap* base_filter) {
    if (row_count < 0 || (base_filter && base_filter->size() !=
                                             static_cast<size_t>(row_count))) {
        return std::nullopt;
    }
    std::unordered_map<GroupKey<T>, size_t> group_ids;
    for (size_t i = 0; i < groups.size(); ++i) {
        if (!group_ids.emplace(groups[i], i).second) {
            return std::nullopt;
        }
    }
    std::vector<std::vector<int64_t>> offsets(groups.size());
    if (!ScanRawField<T>(op_ctx,
                         segment,
                         field_id,
                         row_count,
                         [&](size_t offset, const auto& group) {
                             if (IsEligible(base_filter, offset)) {
                                 auto it = group_ids.find(group);
                                 if (it != group_ids.end()) {
                                     offsets[it->second].push_back(offset);
                                 }
                             }
                         })) {
        return std::nullopt;
    }
    return offsets;
}

#define INSTANTIATE_GROUP_OFFSETS(T)                               \
    template std::optional<std::vector<std::vector<int64_t>>>      \
    BuildGroupOffsets<T>(milvus::OpContext*,                       \
                         const segcore::SegmentInternalInterface&, \
                         FieldId,                                  \
                         int64_t,                                  \
                         const std::vector<std::optional<T>>&,     \
                         const TargetBitmap*);

INSTANTIATE_GROUP_OFFSETS(bool)
INSTANTIATE_GROUP_OFFSETS(int8_t)
INSTANTIATE_GROUP_OFFSETS(int16_t)
INSTANTIATE_GROUP_OFFSETS(int32_t)
INSTANTIATE_GROUP_OFFSETS(int64_t)
INSTANTIATE_GROUP_OFFSETS(std::string)
#undef INSTANTIATE_GROUP_OFFSETS

}  // namespace milvus::exec
