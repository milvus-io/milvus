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
#include "segcore/SegmentChunkReader.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include "cachinglayer/CacheSlot.h"
#include "common/EasyAssert.h"
#include "common/Span.h"
#include "common/type_c.h"
#include "folly/FBVector.h"
#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "segcore/SegcoreConfig.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"

namespace milvus::segcore {
namespace {
std::pair<const index::IndexBase*, int64_t>
GetIndexAndBaseOffset(const SegmentInternalInterface* segment,
                      FieldId field_id,
                      int chunk_id,
                      PinnedIndexView pinned_index) {
    if (pinned_index.empty()) {
        return {nullptr, 0};
    }

    if (chunk_id >= 0 && segment->type() == SegmentType::Sealed &&
        segment->is_chunked() && pinned_index.size() == 1) {
        auto base_offset =
            chunk_id == 0 ? 0
                          : segment->num_rows_until_chunk(field_id, chunk_id);
        return {pinned_index[0].get(), base_offset};
    }

    if (chunk_id >= 0 && static_cast<size_t>(chunk_id) < pinned_index.size()) {
        return {pinned_index[static_cast<size_t>(chunk_id)].get(), 0};
    }

    return {nullptr, 0};
}
}  // namespace

struct ColumnScanState::State {
    std::shared_ptr<ChunkedColumnInterface> column;
    ScanResult cursor;
    TargetType target_type = TargetType::None;
};

template <typename T>
MultipleChunkDataAccessor
SegmentChunkReader::GetMultipleChunkScanDataAccessor(
    FieldId field_id,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos,
    int64_t scan_batch_size,
    ColumnScanState* scan_state,
    std::shared_ptr<ChunkedColumnInterface> column) const {
    AssertInfo(scan_batch_size > 0, "column scan batch size must be positive");
    AssertInfo(column != nullptr, "field {} has no column", field_id.get());
    const auto start =
        NumRowsUntilChunk(field_id, current_chunk_id) + current_chunk_pos;
    const auto target_type = TargetTypeOf<T>();
    auto scan = scan_state ? scan_state->state_ : nullptr;
    if (!scan) {
        scan = std::make_shared<ColumnScanState::State>();
        scan->column = std::move(column);
        scan->target_type = target_type;
        const auto pin_policy =
            SegcoreConfig::default_config().get_scan_cursor_owns_pin()
                ? ScanPinPolicy::CursorOwned
                : ScanPinPolicy::ResultOwned;
        scan->cursor = scan->column->Scan(
            op_ctx_, ScanOptions::ForData(start, target_type, pin_policy));
        AssertInfo(scan->cursor != nullptr,
                   "field {} does not support data Scan",
                   field_id.get());
        if (scan_state) {
            scan_state->state_ = scan;
        }
    } else {
        AssertInfo(scan->target_type == target_type,
                   "field {} scan target changed from {} to {}",
                   field_id.get(),
                   static_cast<int>(scan->target_type),
                   static_cast<int>(target_type));
        if (scan->cursor->Position() != start) {
            // MoveCursor can skip execution windows without reading data.
            scan->cursor->Seek(start);
        }
    }

    struct BatchState {
        ScanBatch batch;
        const T* values = nullptr;
        int64_t batch_pos = 0;
    };
    auto state = std::make_shared<BatchState>();
    const auto num_chunks = NumChunkData(field_id);
    auto current_chunk_size = ChunkSize(field_id, current_chunk_id);
    return [=,
            this,
            window_remaining = scan_batch_size,
            &current_chunk_id,
            &current_chunk_pos]() mutable -> const data_access_type {
        if (state->batch_pos == state->batch.size) {
            if (window_remaining == 0) {
                window_remaining = scan_batch_size;
            }
            const auto remaining = active_count_ - scan->cursor->Position();
            const auto has_data =
                scan->cursor->Next(std::min(window_remaining, remaining),
                                   ScanReadMode::DataAndValidity,
                                   &state->batch);
            AssertInfo(has_data && state->batch.size > 0,
                       "column scan exhausted before accessor consumption");
            state->batch_pos = 0;
            state->values = state->batch.values.template data_as<T>();
        }
        while (current_chunk_pos >= current_chunk_size) {
            ++current_chunk_id;
            current_chunk_pos = 0;
            AssertInfo(current_chunk_id < num_chunks,
                       "field {} cursor chunk_id {} exceeds num_chunks {}",
                       field_id.get(),
                       current_chunk_id,
                       num_chunks);
            current_chunk_size = ChunkSize(field_id, current_chunk_id);
        }
        ++current_chunk_pos;
        --window_remaining;
        const auto pos = state->batch_pos++;
        if (state->batch.validity && !state->batch.validity[pos]) {
            return std::nullopt;
        }
        return data_access_type(state->values[pos]);
    };
}

template <typename T>
MultipleChunkDataAccessor
SegmentChunkReader::GetMultipleChunkDataAccessor(
    FieldId field_id,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos,
    PinnedIndexView pinned_index,
    int64_t scan_batch_size,
    ColumnScanState* scan_state) const {
    const index::IndexBase* index = nullptr;
    if (current_chunk_id < pinned_index.size()) {
        index = pinned_index[current_chunk_id].get();
    }
    if (index) {
        auto index_ptr = dynamic_cast<const index::ScalarIndex<T>*>(index);
        if (index_ptr->HasRawData()) {
            return
                [&,
                 index_ptr = std::move(index_ptr)]() -> const data_access_type {
                    if (current_chunk_pos >= active_count_) {
                        return std::nullopt;
                    }
                    auto raw = index_ptr->Reverse_Lookup(current_chunk_pos++);
                    if (!raw.has_value()) {
                        return std::nullopt;
                    }
                    return raw.value();
                };
        }
    }
    if (segment_->type() == SegmentType::Sealed) {
        auto column = GetColumn(field_id);
        if (column != nullptr &&
            column->GetLocalFormat() ==
                ChunkedColumnInterface::LocalFormat::Vortex) {
            return GetMultipleChunkScanDataAccessor<T>(field_id,
                                                       current_chunk_id,
                                                       current_chunk_pos,
                                                       scan_batch_size,
                                                       scan_state,
                                                       std::move(column));
        }
    }
    auto num_chunks = NumChunkData(field_id);
    AssertInfo(current_chunk_id < num_chunks,
               "field {} cursor chunk_id {} exceeds num_chunks {}",
               field_id.get(),
               current_chunk_id,
               num_chunks);
    // pw is captured by value, each time we need to access a new chunk, we need to
    // pin a new Chunk.
    auto pw = segment_->chunk_data<T>(op_ctx_, field_id, current_chunk_id);
    auto chunk_info = pw.get();
    auto chunk_data = chunk_info.data();
    auto chunk_validity = chunk_info.validity();
    auto current_chunk_size = ChunkSize(field_id, current_chunk_id);
    return [=,
            this,
            pw = std::move(pw),
            &current_chunk_id,
            &current_chunk_pos]() mutable -> const data_access_type {
        if (current_chunk_pos >= current_chunk_size) {
            current_chunk_id++;
            current_chunk_pos = 0;
            AssertInfo(current_chunk_id < num_chunks,
                       "field {} cursor chunk_id {} exceeds num_chunks {}",
                       field_id.get(),
                       current_chunk_id,
                       num_chunks);
            // the old chunk will be unpinned, pw will now pin the new chunk.
            pw = segment_->chunk_data<T>(op_ctx_, field_id, current_chunk_id);
            chunk_data = pw.get().data();
            chunk_validity = pw.get().validity();
            current_chunk_size = ChunkSize(field_id, current_chunk_id);
        }
        if (chunk_validity && !chunk_validity[current_chunk_pos]) {
            current_chunk_pos++;
            return std::nullopt;
        }
        return chunk_data[current_chunk_pos++];
    };
}

MultipleChunkDataAccessor
SegmentChunkReader::GetMultipleChunkStringDataAccessor(
    FieldId field_id,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos,
    PinnedIndexView pinned_index,
    int64_t scan_batch_size,
    ColumnScanState* scan_state) const {
    const index::IndexBase* index = nullptr;
    if (current_chunk_id < pinned_index.size()) {
        index = pinned_index[current_chunk_id].get();
    }
    if (index) {
        auto index_ptr =
            dynamic_cast<const index::ScalarIndex<std::string>*>(index);
        if (index_ptr->HasRawData()) {
            return [&, index_ptr = std::move(index_ptr)]() mutable
                   -> const data_access_type {
                if (current_chunk_pos >= active_count_) {
                    return std::nullopt;
                }
                auto raw = index_ptr->Reverse_Lookup(current_chunk_pos++);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
        }
    }
    auto num_chunks = NumChunkData(field_id);
    AssertInfo(current_chunk_id < num_chunks,
               "field {} cursor chunk_id {} exceeds num_chunks {}",
               field_id.get(),
               current_chunk_id,
               num_chunks);
    if (segment_->type() == SegmentType::Sealed) {
        return GetMultipleChunkScanDataAccessor<std::string_view>(
            field_id,
            current_chunk_id,
            current_chunk_pos,
            scan_batch_size,
            scan_state,
            GetColumn(field_id));
    }
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto pw = segment_->chunk_data<std::string>(
            op_ctx_, field_id, current_chunk_id);
        auto chunk_info = pw.get();
        auto chunk_data = chunk_info.data();
        auto chunk_validity = chunk_info.validity();
        auto current_chunk_size = ChunkSize(field_id, current_chunk_id);
        return [pw = std::move(pw),
                this,
                field_id,
                chunk_data,
                chunk_validity,
                current_chunk_size,
                num_chunks,
                // pw = std::move(pw),
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                AssertInfo(current_chunk_id < num_chunks,
                           "field {} cursor chunk_id {} exceeds num_chunks {}",
                           field_id.get(),
                           current_chunk_id,
                           num_chunks);
                pw = segment_->chunk_data<std::string>(
                    op_ctx_, field_id, current_chunk_id);
                chunk_data = pw.get().data();
                chunk_validity = pw.get().validity();
                current_chunk_size = ChunkSize(field_id, current_chunk_id);
            }
            if (chunk_validity && !chunk_validity[current_chunk_pos]) {
                current_chunk_pos++;
                return std::nullopt;
            }
            return data_access_type(
                std::string_view(chunk_data[current_chunk_pos++]));
        };
    } else {
        auto pw = segment_->chunk_view<std::string_view>(
            op_ctx_, field_id, current_chunk_id);
        auto current_chunk_size = ChunkSize(field_id, current_chunk_id);
        return [=,
                this,
                pw = std::move(pw),
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                AssertInfo(current_chunk_id < num_chunks,
                           "field {} cursor chunk_id {} exceeds num_chunks {}",
                           field_id.get(),
                           current_chunk_id,
                           num_chunks);
                pw = segment_->chunk_view<std::string_view>(
                    op_ctx_, field_id, current_chunk_id);
                current_chunk_size = ChunkSize(field_id, current_chunk_id);
            }
            auto& chunk_data = pw.get().first;
            auto& chunk_valid_data = pw.get().second;
            if (chunk_valid_data && !chunk_valid_data[current_chunk_pos]) {
                current_chunk_pos++;
                return std::nullopt;
            }
            return data_access_type(chunk_data[current_chunk_pos++]);
        };
    }
}

MultipleChunkDataAccessor
SegmentChunkReader::GetMultipleChunkDataAccessor(
    DataType data_type,
    FieldId field_id,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos,
    PinnedIndexView pinned_index,
    int64_t scan_batch_size,
    ColumnScanState* scan_state) const {
    switch (data_type) {
        case DataType::BOOL:
            return GetMultipleChunkDataAccessor<bool>(field_id,
                                                      current_chunk_id,
                                                      current_chunk_pos,
                                                      pinned_index,
                                                      scan_batch_size,
                                                      scan_state);
        case DataType::INT8:
            return GetMultipleChunkDataAccessor<int8_t>(field_id,
                                                        current_chunk_id,
                                                        current_chunk_pos,
                                                        pinned_index,
                                                        scan_batch_size,
                                                        scan_state);
        case DataType::INT16:
            return GetMultipleChunkDataAccessor<int16_t>(field_id,
                                                         current_chunk_id,
                                                         current_chunk_pos,
                                                         pinned_index,
                                                         scan_batch_size,
                                                         scan_state);
        case DataType::INT32:
            return GetMultipleChunkDataAccessor<int32_t>(field_id,
                                                         current_chunk_id,
                                                         current_chunk_pos,
                                                         pinned_index,
                                                         scan_batch_size,
                                                         scan_state);
        case DataType::INT64:
            return GetMultipleChunkDataAccessor<int64_t>(field_id,
                                                         current_chunk_id,
                                                         current_chunk_pos,
                                                         pinned_index,
                                                         scan_batch_size,
                                                         scan_state);
        case DataType::TIMESTAMPTZ:
            return GetMultipleChunkDataAccessor<int64_t>(field_id,
                                                         current_chunk_id,
                                                         current_chunk_pos,
                                                         pinned_index,
                                                         scan_batch_size,
                                                         scan_state);
        case DataType::FLOAT:
            return GetMultipleChunkDataAccessor<float>(field_id,
                                                       current_chunk_id,
                                                       current_chunk_pos,
                                                       pinned_index,
                                                       scan_batch_size,
                                                       scan_state);
        case DataType::DOUBLE:
            return GetMultipleChunkDataAccessor<double>(field_id,
                                                        current_chunk_id,
                                                        current_chunk_pos,
                                                        pinned_index,
                                                        scan_batch_size,
                                                        scan_state);
        case DataType::VARCHAR:
        case DataType::TEXT: {
            return GetMultipleChunkStringDataAccessor(field_id,
                                                      current_chunk_id,
                                                      current_chunk_pos,
                                                      pinned_index,
                                                      scan_batch_size,
                                                      scan_state);
        }
        default:
            ThrowInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

template <typename T>
ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(FieldId field_id,
                                         int chunk_id,
                                         PinnedIndexView pinned_index) const {
    auto index_and_base_offset =
        GetIndexAndBaseOffset(segment_, field_id, chunk_id, pinned_index);
    auto index = index_and_base_offset.first;
    auto base_offset = index_and_base_offset.second;
    auto index_ptr = dynamic_cast<const index::ScalarIndex<T>*>(index);
    if (index_ptr != nullptr && index_ptr->HasRawData()) {
        return
            [index_ptr, base_offset](int i) mutable -> const data_access_type {
                auto raw = index_ptr->Reverse_Lookup(base_offset + i);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
    }
    auto num_chunks = NumChunkData(field_id);
    AssertInfo(chunk_id >= 0 && chunk_id < num_chunks,
               "field {} chunk_id {} exceeds raw data chunks {}",
               field_id.get(),
               chunk_id,
               num_chunks);
    auto pw = segment_->chunk_data<T>(op_ctx_, field_id, chunk_id);
    return [pw = std::move(pw)](int i) mutable -> const data_access_type {
        auto chunk_info = pw.get();
        auto chunk_data = chunk_info.data();
        auto chunk_validity = chunk_info.validity();
        if (chunk_validity && !chunk_validity[i]) {
            return std::nullopt;
        }
        return chunk_data[i];
    };
}

template <>
ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor<std::string>(
    FieldId field_id, int chunk_id, PinnedIndexView pinned_index) const {
    auto index_and_base_offset =
        GetIndexAndBaseOffset(segment_, field_id, chunk_id, pinned_index);
    auto index = index_and_base_offset.first;
    auto base_offset = index_and_base_offset.second;
    auto index_ptr =
        dynamic_cast<const index::ScalarIndex<std::string>*>(index);
    if (index_ptr != nullptr && index_ptr->HasRawData()) {
        return
            [index_ptr, base_offset](int i) mutable -> const data_access_type {
                auto raw = index_ptr->Reverse_Lookup(base_offset + i);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
    }
    auto num_chunks = NumChunkData(field_id);
    AssertInfo(chunk_id >= 0 && chunk_id < num_chunks,
               "field {} chunk_id {} exceeds raw data chunks {}",
               field_id.get(),
               chunk_id,
               num_chunks);
    if (segment_->type() == SegmentType::Sealed) {
        auto column = GetColumn(field_id);
        AssertInfo(column != nullptr, "string field has no column");
        auto pin = column->GetChunk(op_ctx_, chunk_id);
        return [pin = std::move(pin)](int i) -> const data_access_type {
            const auto* chunk = static_cast<const StringChunk*>(pin.get());
            if (!chunk->isValid(i)) {
                return std::nullopt;
            }
            return data_access_type((*chunk)[i]);
        };
    }
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto pw =
            segment_->chunk_data<std::string>(op_ctx_, field_id, chunk_id);
        return [pw = std::move(pw)](int i) mutable -> const data_access_type {
            auto chunk_data = pw.get().data();
            auto chunk_validity = pw.get().validity();
            if (chunk_validity && !chunk_validity[i]) {
                return std::nullopt;
            }
            return data_access_type(std::string_view(chunk_data[i]));
        };
    } else {
        auto pw =
            segment_->chunk_view<std::string_view>(op_ctx_, field_id, chunk_id);
        return [pw = std::move(pw)](int i) mutable -> const data_access_type {
            auto& chunk_data = pw.get().first;
            auto& chunk_valid_data = pw.get().second;
            if (chunk_valid_data && !chunk_valid_data[i]) {
                return std::nullopt;
            }
            return data_access_type(chunk_data[i]);
        };
    }
}

ChunkDataAccessor
SegmentChunkReader::GetStringDataAccessorByOffsets(
    FieldId field_id, OffsetView offsets, PinnedIndexView pinned_index) const {
    AssertInfo(segment_->type() == SegmentType::Sealed,
               "string Take accessor requires a sealed segment");
    if (!pinned_index.empty()) {
        const auto* index =
            dynamic_cast<const index::ScalarIndex<std::string>*>(
                pinned_index.front().get());
        if (index != nullptr && index->HasRawData()) {
            // A sealed scalar index addresses the entire field. Keep its
            // reverse lookup and the caller-owned pin instead of loading raw.
            return [index, offsets](int i) -> const data_access_type {
                auto value = index->Reverse_Lookup(offsets[i]);
                return value.has_value() ? data_access_type(std::move(*value))
                                         : std::nullopt;
            };
        }
    }
    auto column = GetColumn(field_id);
    AssertInfo(column != nullptr, "string field has no column");
    std::shared_ptr<TakeResult> take =
        column->Take(op_ctx_, TakeOptions{offsets, TargetType::StringView});
    AssertInfo(take != nullptr, "string field does not support Take");
    auto values = take->Access<std::string_view>();
    return [take = std::move(take), values](int i) -> const data_access_type {
        auto item = values[i];
        return item.is_valid ? data_access_type(*item.value) : std::nullopt;
    };
}

template <typename T>
ChunkDataAccessor
SegmentChunkReader::GetVortexDataAccessorByOffsets(FieldId field_id,
                                                   OffsetView offsets) const {
    auto column = GetColumn(field_id);
    if (column == nullptr || column->GetLocalFormat() !=
                                 ChunkedColumnInterface::LocalFormat::Vortex) {
        return {};
    }
    std::shared_ptr<TakeResult> take =
        column->Take(op_ctx_, TakeOptions{offsets, TargetTypeOf<T>()});
    AssertInfo(take != nullptr, "Vortex field does not support Take");
    auto values = take->Access<T>();
    return [take = std::move(take), values](int i) -> const data_access_type {
        auto item = values[i];
        return item.is_valid ? data_access_type(*item.value) : std::nullopt;
    };
}

ChunkDataAccessor
SegmentChunkReader::GetDataAccessorByOffsets(
    DataType data_type,
    FieldId field_id,
    OffsetView offsets,
    PinnedIndexView pinned_index) const {
    AssertInfo(segment_->type() == SegmentType::Sealed,
               "Take accessor requires a sealed segment");
    if (data_type == DataType::VARCHAR || data_type == DataType::STRING ||
        data_type == DataType::TEXT) {
        return GetStringDataAccessorByOffsets(field_id, offsets, pinned_index);
    }
    // Preserve the existing per-chunk scalar-index lookup when raw index data
    // is available. Otherwise Vortex can read the complete offset input once.
    if (!pinned_index.empty()) {
        return {};
    }
    switch (data_type) {
        case DataType::BOOL:
            return GetVortexDataAccessorByOffsets<bool>(field_id, offsets);
        case DataType::INT8:
            return GetVortexDataAccessorByOffsets<int8_t>(field_id, offsets);
        case DataType::INT16:
            return GetVortexDataAccessorByOffsets<int16_t>(field_id, offsets);
        case DataType::INT32:
            return GetVortexDataAccessorByOffsets<int32_t>(field_id, offsets);
        case DataType::TIMESTAMPTZ:
        case DataType::INT64:
            return GetVortexDataAccessorByOffsets<int64_t>(field_id, offsets);
        case DataType::FLOAT:
            return GetVortexDataAccessorByOffsets<float>(field_id, offsets);
        case DataType::DOUBLE:
            return GetVortexDataAccessorByOffsets<double>(field_id, offsets);
        default:
            return {};
    }
}

ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(DataType data_type,
                                         FieldId field_id,
                                         int chunk_id,
                                         PinnedIndexView pinned_index) const {
    switch (data_type) {
        case DataType::BOOL:
            return GetChunkDataAccessor<bool>(field_id, chunk_id, pinned_index);
        case DataType::INT8:
            return GetChunkDataAccessor<int8_t>(
                field_id, chunk_id, pinned_index);
        case DataType::INT16:
            return GetChunkDataAccessor<int16_t>(
                field_id, chunk_id, pinned_index);
        case DataType::INT32:
            return GetChunkDataAccessor<int32_t>(
                field_id, chunk_id, pinned_index);
        case DataType::TIMESTAMPTZ:
        case DataType::INT64:
            return GetChunkDataAccessor<int64_t>(
                field_id, chunk_id, pinned_index);
        case DataType::FLOAT:
            return GetChunkDataAccessor<float>(
                field_id, chunk_id, pinned_index);
        case DataType::DOUBLE:
            return GetChunkDataAccessor<double>(
                field_id, chunk_id, pinned_index);
        case DataType::VARCHAR:
        case DataType::TEXT: {
            return GetChunkDataAccessor<std::string>(
                field_id, chunk_id, pinned_index);
        }
        default:
            ThrowInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

}  // namespace milvus::segcore
