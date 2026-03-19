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

#include "segcore/storagev2translator/SystemIndexTranslator.h"

#include <algorithm>
#include <fmt/core.h>
#include <numeric>
#include <utility>

#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "segcore/Utils.h"

namespace milvus::segcore::storagev2translator {

namespace {

int64_t
estimate_timestamp_index_bytes(int64_t num_rows, int64_t num_chunks) {
    auto num_slices = std::max<int64_t>(1, (num_rows + 4095) / 4096);
    return sizeof(TimestampIndex) +
           num_slices * static_cast<int64_t>(sizeof(int64_t) * 2 +
                                             sizeof(Timestamp)) +
           num_chunks * static_cast<int64_t>(sizeof(const Timestamp*) +
                                             sizeof(int64_t));
}

TimestampIndex
build_timestamp_index(const Timestamp* data, size_t num_rows) {
    TimestampIndex index;
    auto min_slice_length = num_rows < 4096 ? 1 : 4096;
    auto meta = GenerateFakeSlices(data, num_rows, min_slice_length);
    index.set_length_meta(std::move(meta));
    index.build_with(data, num_rows);
    return index;
}

std::unique_ptr<OffsetMap>
create_offset_map(DataType data_type) {
    switch (data_type) {
        case DataType::INT64:
            return std::make_unique<OffsetOrderedArray<int64_t>>();
        case DataType::VARCHAR:
            return std::make_unique<OffsetOrderedArray<std::string>>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported primary key data type {}", data_type);
    }
    return nullptr;
}

int64_t
estimate_pk_index_bytes(DataType data_type,
                        int64_t num_rows,
                        bool is_sorted_by_pk) {
    int64_t base = 0;
    switch (data_type) {
        case DataType::INT64:
            if (is_sorted_by_pk) {
                // sorted: only compressed offset->pk, no pk->offset
                base = num_rows * static_cast<int64_t>(sizeof(int64_t));
            } else {
                // unsorted: both pk->offset and compressed offset->pk
                base = num_rows * static_cast<int64_t>(sizeof(int64_t) * 2);
            }
            break;
        case DataType::VARCHAR:
            if (is_sorted_by_pk) {
                base = 0;  // sorted varchar: no index built
            } else {
                base = num_rows * static_cast<int64_t>(sizeof(std::string) + 16);
            }
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported primary key data type {}", data_type);
    }
    return std::max<int64_t>(base, 1024);
}

}  // namespace

TimestampIndexCell::TimestampIndexCell(TimestampIndex timestamp_index,
                                       int64_t num_rows)
    : timestamp_index_(std::move(timestamp_index)),
      byte_size_{estimate_timestamp_index_bytes(num_rows, 1), 0} {
}

PkIndexCell::PkIndexCell(std::unique_ptr<OffsetMap> pk2offset,
                         std::unique_ptr<CompressedInt64PkArray> offset2pk,
                         bool is_int64_pk)
    : pk2offset_(std::move(pk2offset)),
      offset2pk_(std::move(offset2pk)),
      is_int64_pk_(is_int64_pk),
      byte_size_{static_cast<int64_t>(
                     (pk2offset_ ? pk2offset_->memory_size() : 0) +
                     (offset2pk_ ? offset2pk_->memory_size() : 0)),
                 0} {
}

void
PkIndexCell::bulk_get_int64_pks_by_offsets(const int64_t* offsets,
                                           int64_t count,
                                           int64_t* output) const {
    AssertInfo(has_int64_pk_index(),
               "bulk_get_int64_pks_by_offsets requires int64 PK index");
    offset2pk_->bulk_at(offsets, count, output);
}

TimestampIndexTranslator::TimestampIndexTranslator(
    int64_t segment_id,
    std::shared_ptr<ChunkedColumnInterface> column,
    int64_t num_rows)
    : segment_id_(segment_id),
      column_(std::move(column)),
      num_rows_(num_rows),
      key_(fmt::format("seg_{}_ts_index", segment_id)),
      meta_(milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::cachinglayer::CellDataType::OTHER,
            CacheWarmupPolicy::CacheWarmupPolicy_Disable,
            /* support_eviction */ true) {
}

size_t
TimestampIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
TimestampIndexTranslator::cell_id_of(milvus::cachinglayer::uid_t) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
TimestampIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t) const {
    return {{estimate_timestamp_index_bytes(num_rows_, column_->num_chunks()),
             0},
            {0, 0}};
}

const std::string&
TimestampIndexTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<TimestampIndexCell>>>
TimestampIndexTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>&) {
    CheckCancellation(
        ctx, segment_id_, "TimestampIndexTranslator::get_cells()");

    // Pin column chunks temporarily to build the TimestampIndex, then release.
    // TimestampIndexCell intentionally does NOT hold raw timestamp data or
    // pins to GroupChunk cells — callers read raw timestamps by pinning the
    // timestamp column directly. This avoids a DList deadlock where evicting
    // this cell would unpin GroupChunk cells under the same list_mtx_.
    auto all_chunks = column_->GetAllChunks(ctx);
    TimestampIndex index;
    if (all_chunks.size() == 1) {
        auto* fixed_chunk = static_cast<FixedWidthChunk*>(all_chunks[0].get());
        auto span = fixed_chunk->Span();
        auto* ts_ptr = static_cast<const Timestamp*>(span.data());
        AssertInfo(static_cast<int64_t>(span.row_count()) == num_rows_,
                   "timestamp chunk row count {} != expected {}",
                   span.row_count(),
                   num_rows_);
        index = build_timestamp_index(ts_ptr, num_rows_);
    } else {
        std::vector<Timestamp> temp(num_rows_);
        size_t offset = 0;
        for (auto& pin : all_chunks) {
            auto* fixed_chunk = static_cast<FixedWidthChunk*>(pin.get());
            auto span = fixed_chunk->Span();
            std::copy_n(static_cast<const Timestamp*>(span.data()),
                        span.row_count(),
                        temp.data() + offset);
            offset += span.row_count();
        }
        AssertInfo(static_cast<int64_t>(offset) == num_rows_,
                   "timestamp total row count {} != expected {}",
                   offset,
                   num_rows_);
        index = build_timestamp_index(temp.data(), num_rows_);
    }

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<TimestampIndexCell>>>
        result;
    result.emplace_back(
        0, std::make_unique<TimestampIndexCell>(std::move(index), num_rows_));
    return result;
}

Meta*
TimestampIndexTranslator::meta() {
    return &meta_;
}

PkIndexTranslator::PkIndexTranslator(int64_t segment_id,
                                     std::shared_ptr<ChunkedColumnInterface> column,
                                     DataType data_type,
                                     bool is_sorted_by_pk)
    : segment_id_(segment_id),
      column_(std::move(column)),
      data_type_(data_type),
      is_sorted_by_pk_(is_sorted_by_pk),
      key_(fmt::format("seg_{}_pk_index", segment_id)),
      meta_(milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::cachinglayer::CellDataType::OTHER,
            CacheWarmupPolicy::CacheWarmupPolicy_Disable,
            /* support_eviction */ true) {
}

size_t
PkIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
PkIndexTranslator::cell_id_of(milvus::cachinglayer::uid_t) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
PkIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t) const {
    return {{estimate_pk_index_bytes(data_type_, column_->NumRows(), is_sorted_by_pk_),
             0},
            {0, 0}};
}

const std::string&
PkIndexTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<PkIndexCell>>>
PkIndexTranslator::get_cells(milvus::OpContext* ctx,
                             const std::vector<milvus::cachinglayer::cid_t>&) {
    CheckCancellation(ctx, segment_id_, "PkIndexTranslator::get_cells()");

    std::unique_ptr<OffsetMap> pk2offset;
    std::unique_ptr<CompressedInt64PkArray> offset2pk;

    auto num_chunks = column_->num_chunks();
    std::vector<int64_t> chunk_ids(num_chunks);
    std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
    column_->PrefetchChunks(ctx, chunk_ids);

    int64_t offset = 0;
    switch (data_type_) {
        case DataType::INT64: {
            std::vector<int64_t> all_pks;
            all_pks.reserve(column_->NumRows());
            if (!is_sorted_by_pk_) {
                pk2offset = create_offset_map(data_type_);
            }
            for (int64_t i = 0; i < num_chunks; ++i) {
                auto pw = column_->DataOfChunk(ctx, i);
                auto pks = reinterpret_cast<const int64_t*>(pw.get());
                auto chunk_num_rows = column_->chunk_row_nums(i);
                for (int64_t j = 0; j < chunk_num_rows; ++j) {
                    auto pk = pks[j];
                    all_pks.push_back(pk);
                    if (pk2offset) {
                        pk2offset->insert(pk, offset);
                    }
                    ++offset;
                }
            }
            offset2pk = std::make_unique<CompressedInt64PkArray>();
            offset2pk->build(all_pks.data(), all_pks.size());
            break;
        }
        case DataType::VARCHAR: {
            if (!is_sorted_by_pk_) {
                pk2offset = create_offset_map(data_type_);
            }
            for (int64_t i = 0; i < num_chunks; ++i) {
                auto pw = column_->StringViews(ctx, i);
                auto& pks = pw.get().first;
                for (auto pk : pks) {
                    if (pk2offset) {
                        pk2offset->insert(std::string(pk), offset);
                    }
                    ++offset;
                }
            }
            break;
        }
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported primary key data type {}", data_type_);
    }

    if (pk2offset) {
        pk2offset->seal();
    }

    std::vector<std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<PkIndexCell>>>
        result;
    result.emplace_back(
        0,
        std::make_unique<PkIndexCell>(
            std::move(pk2offset), std::move(offset2pk), data_type_ == DataType::INT64));
    return result;
}

Meta*
PkIndexTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev2translator
