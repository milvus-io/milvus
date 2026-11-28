// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <algorithm>
#include <fmt/format.h>
#include <limits>
#include <unordered_map>
#include "grep/Grep.h"
#include "grep/IndexCondition.h"
#include "grep/SegmentGrepReader.h"
#include "log/Log.h"
#include "index/contracts/query/INgramReader.h"
#include "milvus-storage/lob_column/lob_reference.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentInterface.h"

namespace milvus::grep {
using namespace milvus::segcore;
std::optional<std::string_view>
GrepInputBatch::Text(int64_t index) const {
    if (!inline_text.empty()) {
        return inline_text[index];
    }
    if (pinned) {
        const auto& [views, validity] = pinned->get();
        if (!validity.empty() && !validity[index]) {
            return std::nullopt;
        }
        return views[index];
    }
    if (!owned_positions.empty()) {
        index = owned_positions[index];
    }
    const auto& validity = GetFieldDataRowValidData(*owned);
    if (!validity.empty() && !validity[index]) {
        return std::nullopt;
    }
    return owned->scalars().string_data().data(index);
}

namespace {
// Borrow only self-contained inline TEXT. If any non-null value is a LOB
// reference, leave the entire batch to the existing bulk reader so file-group
// batching and prefetch/error behavior stay unchanged. Keep every chunk pinned
// until matching and Arrow result copying have both completed.
bool
TryReadInlineTextBatch(const ChunkedColumnInterface* column,
                       milvus::OpContext* op_ctx,
                       const int64_t* offsets,
                       GrepInputBatch& batch) {
    const auto& boundaries = column->GetNumRowsUntilChunk();
    batch.inline_text.resize(batch.count);
    int64_t begin = 0;
    while (begin < batch.count) {
        const auto end = std::upper_bound(
            boundaries.begin(), boundaries.end(), offsets[begin]);
        AssertInfo(end != boundaries.begin() && end != boundaries.end(),
                   "grep offset out of column range");
        const int64_t chunk = std::distance(boundaries.begin(), end) - 1;
        const int64_t chunk_begin = *(end - 1), chunk_end = *end;
        auto stop = begin + 1;
        while (stop < batch.count && offsets[stop] >= chunk_begin &&
               offsets[stop] < chunk_end) {
            ++stop;
        }
        FixedVector<int32_t> local_offsets(stop - begin);
        for (auto i = begin; i < stop; ++i) {
            const auto relative = offsets[i] - chunk_begin;
            AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                       "grep chunk offset exceeds int32");
            local_offsets[i - begin] = relative;
        }
        auto pinned =
            column->StringViewsByOffsets(op_ctx, chunk, local_offsets);
        const auto& [views, validity] = pinned.get();
        for (auto i = begin; i < stop; ++i) {
            const auto local = i - begin;
            if (!validity.empty() && !validity[local])
                continue;
            const auto encoded = views[local];
            if (encoded.empty()) {
                batch.inline_text[i] = std::string_view{};
                continue;
            }
            const auto* data = reinterpret_cast<const uint8_t*>(encoded.data());
            if (!milvus_storage::lob_column::IsInlineData(data)) {
                batch.inline_text.clear();
                batch.text_pins.clear();
                return false;
            }
            const uint8_t* payload = nullptr;
            size_t length = 0;
            milvus_storage::lob_column::DecodeInlineData(
                data, encoded.size(), &payload, &length);
            batch.inline_text[i] =
                length == 0
                    ? std::string_view{}
                    : std::string_view(reinterpret_cast<const char*>(payload),
                                       length);
        }
        batch.text_pins.push_back(std::move(pinned));
        begin = stop;
    }
    return true;
}

}  // namespace

// Compute the original read window before pruning. In particular, keep
// read-ahead offset validation and chunk boundaries independent of candidates.
int64_t
GrepReadCount(const SegmentInternalInterface* segment,
              const FieldMeta& meta,
              const int64_t* offsets,
              int64_t max_rows,
              int64_t rows,
              bool use_views,
              const ChunkedColumnInterface* view_column) {
    AssertInfo(offsets[0] >= 0 && offsets[0] < rows,
               "grep offset out of range");
    const auto field = meta.get_id();
    int64_t chunk_begin = 0, chunk_end = rows;
    if (view_column && meta.get_data_type() != DataType::TEXT) {
        const auto& boundaries = view_column->GetNumRowsUntilChunk();
        const auto end =
            std::upper_bound(boundaries.begin(), boundaries.end(), offsets[0]);
        AssertInfo(end != boundaries.begin() && end != boundaries.end(),
                   "grep offset out of column range");
        chunk_begin = *(end - 1);
        chunk_end = *end;
    }
    const auto chunk =
        !view_column && use_views
            ? segment->get_chunk_by_offset(field, offsets[0]).first
            : 0;
    int64_t count = 1;
    while (count < max_rows) {
        const auto offset = offsets[count];
        AssertInfo(offset >= 0 && offset < rows, "grep offset out of range");
        if (offset < chunk_begin || offset >= chunk_end ||
            (!view_column && use_views &&
             segment->get_chunk_by_offset(field, offset).first != chunk)) {
            break;
        }
        ++count;
    }
    return count;
}

namespace {
// LOB take can coalesce repeated row references. Fetch each input row once,
// then expose repeated views in input order without copying large documents.
void
ReadOwnedGrepBatch(const SegmentInternalInterface* segment,
                   const std::shared_ptr<const SegmentReadSnapshot>& snapshot,
                   milvus::OpContext* op_ctx,
                   const FieldMeta& meta,
                   const int64_t* offsets,
                   GrepInputBatch& batch) {
    const auto end = offsets + batch.count;
    const bool unique_sorted =
        std::adjacent_find(
            offsets, end, [](int64_t a, int64_t b) { return a >= b; }) == end;
    if (meta.get_data_type() != DataType::TEXT || unique_sorted) {
        batch.owned = BulkSubscriptWithSnapshot(segment,
                                                snapshot,
                                                op_ctx,
                                                meta.get_id(),
                                                meta,
                                                offsets,
                                                batch.count);
        return;
    }
    std::unordered_map<int64_t, int64_t> positions;
    std::vector<int64_t> unique_offsets;
    positions.reserve(batch.count);
    unique_offsets.reserve(batch.count);
    batch.owned_positions.reserve(batch.count);
    for (int64_t i = 0; i < batch.count; ++i) {
        auto [entry, inserted] =
            positions.try_emplace(offsets[i], unique_offsets.size());
        if (inserted)
            unique_offsets.push_back(offsets[i]);
        batch.owned_positions.push_back(entry->second);
    }
    batch.owned = BulkSubscriptWithSnapshot(segment,
                                            snapshot,
                                            op_ctx,
                                            meta.get_id(),
                                            meta,
                                            unique_offsets.data(),
                                            unique_offsets.size());
    if (unique_offsets.size() == static_cast<size_t>(batch.count)) {
        batch.owned_positions.clear();
    }
}

}  // namespace

GrepInputBatch
ReadGrepBatch(const SegmentInternalInterface* segment,
              const std::shared_ptr<const SegmentReadSnapshot>& snapshot,
              milvus::OpContext* op_ctx,
              const FieldMeta& meta,
              const int64_t* offsets,
              int64_t count,
              bool use_views,
              const ChunkedColumnInterface* view_column) {
    const auto field = meta.get_id();
    GrepInputBatch batch;
    batch.count = count;
    if (view_column != nullptr && meta.get_data_type() == DataType::TEXT) {
        if (!TryReadInlineTextBatch(view_column, op_ctx, offsets, batch)) {
            ReadOwnedGrepBatch(segment, snapshot, op_ctx, meta, offsets, batch);
        }
        return batch;
    }
    if (view_column != nullptr) {
        const auto& boundaries = view_column->GetNumRowsUntilChunk();
        const auto end =
            std::upper_bound(boundaries.begin(), boundaries.end(), offsets[0]);
        AssertInfo(end != boundaries.begin() && end != boundaries.end(),
                   "grep offset out of column range");
        const int64_t chunk = std::distance(boundaries.begin(), end) - 1;
        const int64_t chunk_begin = *(end - 1);
        FixedVector<int32_t> local_offsets(count);
        for (int64_t i = 0; i < count; ++i) {
            const auto relative = offsets[i] - chunk_begin;
            AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                       "grep chunk offset exceeds int32");
            local_offsets[i] = relative;
        }
        batch.pinned.emplace(
            view_column->StringViewsByOffsets(op_ctx, chunk, local_offsets));
    } else if (use_views) {
        const auto chunk =
            segment->get_chunk_by_offset(field, offsets[0]).first;
        FixedVector<int32_t> local_offsets(count);
        for (int64_t i = 0; i < count; ++i) {
            auto relative =
                segment->get_chunk_by_offset(field, offsets[i]).second;
            AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                       "grep chunk offset exceeds int32");
            local_offsets[i] = relative;
        }
        batch.pinned.emplace(segment->chunk_views_by_offsets<std::string_view>(
            op_ctx, field, chunk, local_offsets));
    } else {
        ReadOwnedGrepBatch(segment, snapshot, op_ctx, meta, offsets, batch);
    }
    return batch;
}

// Creates only sealed, whole-field NGRAM readers with complete row coverage.
// Loading errors propagate; they are not index ineligibility.
std::optional<GrepIndex>
PrepareGrepIndex(const SegmentInternalInterface* segment,
                 milvus::OpContext* op_ctx,
                 const FieldMeta& meta,
                 const Plan& plan,
                 int64_t size,
                 int64_t rows) {
    if (segment->type() != SegmentType::Sealed || size == 0) {
        return std::nullopt;
    }
    const auto registrations = segment->IndexRegistrations(meta.get_id());
    if (std::none_of(
            registrations.begin(), registrations.end(), [](const auto& r) {
                return r.kind == IndexKind::Ngram && r.json_path.empty();
            })) {
        return std::nullopt;
    }
    auto condition = DecodeLiteralConditionTree(plan.ConditionPayload());
    if (condition.IsTrue()) {
        return std::nullopt;
    }
    const auto capabilities =
        segment->ResolveIndexCapability(op_ctx, meta.get_id());
    for (const auto& entry : capabilities.entries()) {
        if (!entry.caps.ngram_candidates || entry.caps.nested ||
            !entry.json_path.empty() ||
            entry.value_type != meta.get_data_type()) {
            continue;
        }
        auto owner = segment->CreateIndexReader(op_ctx, entry.key);
        if (!owner || owner->CoordDomain() != index::Domain::Row ||
            owner->Count() != rows ||
            owner->ValueType() != meta.get_data_type()) {
            continue;
        }
        const auto* reader =
            dynamic_cast<const index::INgramReader*>(owner.get());
        if (!reader || !reader->CanHandleConditionTree(condition)) {
            continue;
        }
        return GrepIndex{std::move(owner), reader, std::move(condition)};
    }
    return std::nullopt;
}

}  // namespace milvus::grep
