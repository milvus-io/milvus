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
#include <string_view>
#include <type_traits>
#include <unordered_set>

#include "exec/expression/IndexPathSelection.h"
#include "exec/expression/ValueLookupSource.h"
#include "index/contracts/query/IScalarValueReader.h"
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

// Index readers express the string families through borrowed views.
template <typename T>
using ReaderValueType =
    std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

// The index-backed membership path, for a field that kept no raw data of its
// own. #53246 reached straight into a pinned `ScalarIndex<T>` and probed it
// with `In()` + `IsNull()`; on the refactor's contracts the value source for a
// group key is `PinnedValueLookup` / `IScalarValueReader<T>` -- the same source
// SealedDataGetter uses for phase one, so an indexed and a raw scan produce
// byte-identical group keys, NULL rows included (Lookup/Gather report a NULL
// row as invalid, which is exactly `GroupKey<T>(std::nullopt)`).
//
// `Gather` rather than per-row `Lookup`: one call lets the reader cluster the
// offsets by its own layout, and its `const T*` views only have to outlive the
// callback.
//
// Every guard below ends in std::nullopt, which makes the caller fall back to
// the unoptimised iterator. The bitmap this returns is flipped and handed to a
// fresh vector iterator as an exclusion filter over [0, row_count) in ROW
// space, so an entry that answers in element space or does not cover the whole
// searched prefix cannot be used -- it would exclude rows that do belong to an
// unfinished group. `cheap_value_lookup` is required because this gathers one
// value per eligible row: a reader whose reverse lookup is not O(1)/O(log n)
// would cost more than the fallback it is trying to avoid.
template <typename T>
std::optional<TargetBitmap>
BuildIndexMembership(milvus::OpContext* op_ctx,
                     const segcore::SegmentInternalInterface& segment,
                     FieldId field_id,
                     size_t row_count,
                     const std::vector<GroupKey<T>>& groups,
                     const TargetBitmap* base_filter) {
    using ReaderType = ReaderValueType<T>;

    auto value_type = segment.GetFieldDataType(field_id);
    if (value_type == DataType::TIMESTAMPTZ) {
        // Indexed the same way every other value-lookup consumer indexes it.
        value_type = DataType::INT64;
    }
    const ExprIndexRequirement requirement{
        .field_id = field_id,
        .reader = RequiredReader::ValueLookup,
        .value_type = value_type,
    };

    // Metadata-only pre-check. PinnedValueLookup repeats this selection, but it
    // does not expose the chosen entry's caps and the cost guard below has to
    // see them before anything is pinned.
    const auto capabilities = segment.IndexCapability(field_id);
    const auto decision = DetermineExecPath(requirement, capabilities);
    if (!decision.key.has_value()) {
        return std::nullopt;
    }
    const auto* entry = capabilities.Find(*decision.key);
    if (entry == nullptr || entry->caps.nested || entry->caps.json_paths ||
        !entry->caps.cheap_value_lookup) {
        return std::nullopt;
    }

    // The pin must outlive every Gather callback below.
    PinnedValueLookup lookup(&segment,
                             op_ctx,
                             field_id,
                             value_type,
                             static_cast<int64_t>(row_count));
    if (!lookup.HasReader()) {
        return std::nullopt;
    }
    const auto* reader = lookup.Reader<ReaderType>();
    if (reader == nullptr) {
        return std::nullopt;
    }

    // Only rows the vector search still considers eligible can join a group --
    // the same decision the raw scan makes with IsEligible, which is why this
    // path needs no separate base-filter subtraction afterwards.
    std::vector<int64_t> offsets;
    offsets.reserve(row_count);
    for (size_t offset = 0; offset < row_count; ++offset) {
        if (IsEligible(base_filter, offset)) {
            offsets.push_back(static_cast<int64_t>(offset));
        }
    }
    const auto count = static_cast<int64_t>(offsets.size());
    // A growing pin covers a contiguous row prefix; a prefix shorter than the
    // searched range cannot answer for the tail.
    if (!lookup.Covers(offsets.data(), count)) {
        return std::nullopt;
    }

    segcore::CheckCancellation(op_ctx,
                               segment.get_segment_id(),
                               field_id.get(),
                               "strict group membership");
    std::unordered_set<GroupKey<T>> target_groups(groups.begin(), groups.end());
    TargetBitmap membership(row_count, false);
    reader->Gather(
        offsets.data(),
        count,
        [&](int64_t i, const ReaderType* value, bool valid) {
            auto group = valid && value != nullptr
                             ? GroupKey<T>(T(*value))
                             : GroupKey<T>(std::nullopt);
            if (target_groups.find(group) != target_groups.end()) {
                membership[offsets[static_cast<size_t>(i)]] = true;
            }
        });
    return membership;
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
                                            chunk_pos);
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
std::optional<TargetBitmap>
BuildGroupMembership(milvus::OpContext* op_ctx,
                     const segcore::SegmentInternalInterface& segment,
                     FieldId field_id,
                     int64_t row_count,
                     const std::vector<GroupKey<T>>& groups,
                     const TargetBitmap* base_filter) {
    if (row_count < 0 ||
        (base_filter != nullptr &&
         base_filter->size() != static_cast<size_t>(row_count))) {
        return std::nullopt;
    }
    auto count = static_cast<size_t>(row_count);
    // Match phase one's raw-first access policy. Do not pin an unused index.
    if (segment.HasFieldData(field_id)) {
        std::unordered_set<GroupKey<T>> target_groups(groups.begin(),
                                                      groups.end());
        TargetBitmap membership(count, false);
        auto scanned = ScanRawField<T>(
            op_ctx, segment, field_id, count, [&](size_t offset, auto group) {
                if (IsEligible(base_filter, offset) &&
                    target_groups.find(group) != target_groups.end()) {
                    membership[offset] = true;
                }
            });
        if (scanned) {
            return membership;
        }
    }
    return BuildIndexMembership<T>(
        op_ctx, segment, field_id, count, groups, base_filter);
}

template std::optional<TargetBitmap>
BuildGroupMembership<bool>(milvus::OpContext*,
                           const segcore::SegmentInternalInterface&,
                           FieldId,
                           int64_t,
                           const std::vector<std::optional<bool>>&,
                           const TargetBitmap*);
template std::optional<TargetBitmap>
BuildGroupMembership<int8_t>(milvus::OpContext*,
                             const segcore::SegmentInternalInterface&,
                             FieldId,
                             int64_t,
                             const std::vector<std::optional<int8_t>>&,
                             const TargetBitmap*);
template std::optional<TargetBitmap>
BuildGroupMembership<int16_t>(milvus::OpContext*,
                              const segcore::SegmentInternalInterface&,
                              FieldId,
                              int64_t,
                              const std::vector<std::optional<int16_t>>&,
                              const TargetBitmap*);
template std::optional<TargetBitmap>
BuildGroupMembership<int32_t>(milvus::OpContext*,
                              const segcore::SegmentInternalInterface&,
                              FieldId,
                              int64_t,
                              const std::vector<std::optional<int32_t>>&,
                              const TargetBitmap*);
template std::optional<TargetBitmap>
BuildGroupMembership<int64_t>(milvus::OpContext*,
                              const segcore::SegmentInternalInterface&,
                              FieldId,
                              int64_t,
                              const std::vector<std::optional<int64_t>>&,
                              const TargetBitmap*);
template std::optional<TargetBitmap>
BuildGroupMembership<std::string>(
    milvus::OpContext*,
    const segcore::SegmentInternalInterface&,
    FieldId,
    int64_t,
    const std::vector<std::optional<std::string>>&,
    const TargetBitmap*);

}  // namespace milvus::exec
