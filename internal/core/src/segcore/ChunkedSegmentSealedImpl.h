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

#pragma once

#include <tbb/concurrent_priority_queue.h>
#include <tbb/concurrent_vector.h>
#include <folly/Synchronized.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ConcurrentVector.h"
#include "DeletedRecord.h"
#include "SealedIndexingRecord.h"
#include "SegmentSealed.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "folly/Synchronized.h"
#include "google/protobuf/message_lite.h"
#include "mmap/Types.h"
#include "common/Types.h"
#include "common/IndexMeta.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/CacheSlot.h"
#include "segcore/IndexConfigGenerator.h"
#include "segcore/SegcoreConfig.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#include "index/json_stats/JsonKeyStats.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/common.pb.h"
#include "milvus-storage/reader.h"

namespace milvus::segcore {

namespace storagev1translator {
class InsertRecordTranslator;
}

using namespace milvus::cachinglayer;

class ChunkedSegmentSealedImpl : public SegmentSealed {
 public:
    explicit ChunkedSegmentSealedImpl(SchemaPtr schema,
                                      IndexMetaPtr index_meta,
                                      const SegcoreConfig& segcore_config,
                                      int64_t segment_id,
                                      bool is_sorted_by_pk = false);
    ~ChunkedSegmentSealedImpl() override;
    void
    LoadIndex(const LoadIndexInfo& info) override;
    void
    LoadFieldData(const LoadFieldDataInfo& info) override;
    void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) override;
    void
    LoadSegmentMeta(
        const milvus::proto::segcore::LoadSegmentMeta& segment_meta) override;
    void
    DropIndex(const FieldId field_id) override;
    void
    DropJSONIndex(const FieldId field_id,
                  const std::string& nested_path) override;
    void
    DropFieldData(const FieldId field_id) override;
    bool
    HasIndex(FieldId field_id) const override;
    bool
    HasFieldData(FieldId field_id) const override;

    std::pair<std::shared_ptr<ChunkedColumnInterface>, bool>
    GetFieldDataIfExist(FieldId field_id) const;

    std::vector<PinWrapper<const index::IndexBase*>>
    PinIndex(milvus::OpContext* op_ctx,
             FieldId field_id,
             bool include_ngram = false) const override {
        auto [scalar_indexings, ngram_fields] =
            lock(folly::wlock(scalar_indexings_), folly::wlock(ngram_fields_));
        if (!include_ngram) {
            if (ngram_fields->find(field_id) != ngram_fields->end()) {
                return {};
            }
        }

        auto iter = scalar_indexings->find(field_id);
        if (iter == scalar_indexings->end()) {
            return {};
        }
        auto ca = SemiInlineGet(iter->second->PinCells(op_ctx, {0}));
        auto index = ca->get_cell_of(0);
        return {PinWrapper<const index::IndexBase*>(ca, index)};
    }

    bool
    Contain(const PkType& pk) const override {
        return insert_record_.contain(pk);
    }

    void
    AddFieldDataInfoForSealed(
        const LoadFieldDataInfo& field_data_info) override;

    int64_t
    get_segment_id() const override {
        return id_;
    }

    bool
    HasRawData(int64_t field_id) const override;

    DataType
    GetFieldDataType(FieldId fieldId) const override;

    void
    RemoveFieldFile(const FieldId field_id) override;

    void
    CreateTextIndex(FieldId field_id) override;

    void
    LoadTextIndex(std::unique_ptr<milvus::proto::indexcgo::LoadTextIndexInfo>
                      info_proto) override;

    void
    LoadJsonStats(FieldId field_id,
                  index::CacheJsonKeyStatsPtr cache_slot) override {
        json_stats_.wlock()->insert({field_id, std::move(cache_slot)});
    }

    PinWrapper<index::JsonKeyStats*>
    GetJsonStats(milvus::OpContext* op_ctx, FieldId field_id) const override {
        auto r = json_stats_.rlock();
        auto it = r->find(field_id);
        if (it == r->end()) {
            return PinWrapper<index::JsonKeyStats*>(nullptr);
        }
        auto ca = SemiInlineGet(it->second->PinCells(op_ctx, {0}));
        auto* stats = ca->get_cell_of(0);
        AssertInfo(stats != nullptr,
                   "json stats cache is corrupted, field_id: {}",
                   field_id.get());
        return PinWrapper<index::JsonKeyStats*>(ca, stats);
    }

    void
    RemoveJsonStats(FieldId field_id) override {
        json_stats_.wlock()->erase(field_id);
    }

    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx, FieldId field_id) const override;

    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         const std::string& nested_path) const override;

    void
    BulkGetJsonData(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    std::function<void(milvus::Json, size_t, bool)> fn,
                    const int64_t* offsets,
                    int64_t count) const override {
        auto column = fields_.rlock()->at(field_id);
        column->BulkRawJsonAt(op_ctx, fn, offsets, count);
    }

    void
    Reopen(SchemaPtr sch) override;

    void
    LazyCheckSchema(SchemaPtr sch) override;

    void
    FinishLoad() override;

 public:
    size_t
    GetMemoryUsageInBytes() const override {
        return stats_.mem_size.load() + deleted_record_.mem_size();
    }

    InsertRecord<true>&
    get_insert_record() override {
        return insert_record_;
    }

    int64_t
    get_row_count() const override;

    int64_t
    get_deleted_count() const override;

    const Schema&
    get_schema() const override;

    void
    pk_range(milvus::OpContext* op_ctx,
             proto::plan::OpType op,
             const PkType& pk,
             BitsetTypeView& bitset) const override;

    void
    search_sorted_pk_range(milvus::OpContext* op_ctx,
                           proto::plan::OpType op,
                           const PkType& pk,
                           BitsetTypeView& bitset) const;

    void
    pk_binary_range(milvus::OpContext* op_ctx,
                    const PkType& lower_pk,
                    bool lower_inclusive,
                    const PkType& upper_pk,
                    bool upper_inclusive,
                    BitsetTypeView& bitset) const override;

    std::unique_ptr<DataArray>
    get_vector(milvus::OpContext* op_ctx,
               FieldId field_id,
               const int64_t* ids,
               int64_t count) const override;

    bool
    is_nullable(FieldId field_id) const override {
        auto& field_meta = schema_->operator[](field_id);
        return field_meta.is_nullable();
    };

    bool
    is_chunked() const override {
        return true;
    }

    void
    search_pks(BitsetType& bitset, const std::vector<PkType>& pks) const;

    void
    search_batch_pks(
        const std::vector<PkType>& pks,
        const std::function<Timestamp(const size_t idx)>& get_timestamp,
        bool include_same_ts,
        const std::function<void(const SegOffset offset, const Timestamp ts)>&
            callback) const;

 public:
    // count of chunk that has raw data
    int64_t
    num_chunk_data(FieldId field_id) const override;

    int64_t
    num_chunk(FieldId field_id) const override;

    // return size_per_chunk for each chunk, renaming against confusion
    int64_t
    size_per_chunk() const override;

    int64_t
    chunk_size(FieldId field_id, int64_t chunk_id) const override;

    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field_id, int64_t offset) const override;

    int64_t
    num_rows_until_chunk(FieldId field_id, int64_t chunk_id) const override;

    SegcoreError
    Delete(int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) override;

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first(int64_t limit, const BitsetType& bitset) const override;

    // Calculate: output[i] = Vec[seg_offset[i]]
    // where Vec is determined from field_offset
    std::unique_ptr<DataArray>
    bulk_subscript(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   const int64_t* seg_offsets,
                   int64_t count) const override;

    std::unique_ptr<DataArray>
    bulk_subscript(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        const int64_t* seg_offsets,
        int64_t count,
        const std::vector<std::string>& dynamic_field_names) const override;

    bool
    is_mmap_field(FieldId id) const override;

    void
    ClearData() override;

    bool
    is_field_exist(FieldId field_id) const override {
        return schema_->get_fields().find(field_id) !=
               schema_->get_fields().end();
    }

    void
    prefetch_chunks(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    const std::vector<int64_t>& chunk_ids) const override;

 protected:
    // blob and row_count
    PinWrapper<SpanBase>
    chunk_data_impl(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    int64_t chunk_id) const override;

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    chunk_string_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    chunk_array_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    chunk_vector_array_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    chunk_string_views_by_offsets(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const override;

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    chunk_array_views_by_offsets(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const override;

    // Calculate: output[i] = Vec[seg_offset[i]],
    // where Vec is determined from field_offset
    void
    bulk_subscript(milvus::OpContext* op_ctx,
                   SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const override;

    void
    check_search(const query::Plan* plan) const override;

    int64_t
    get_active_count(Timestamp ts) const override;

    const ConcurrentVector<Timestamp>&
    get_timestamps() const override {
        return insert_record_.timestamps_;
    }

    // Load Geometry cache for a field
    void
    LoadGeometryCache(FieldId field_id,
                      const std::shared_ptr<ChunkedColumnInterface>& column);

 private:
    void
    drop_field_data_locked(const FieldId field_id);

    // Reload field data from storage (used when index without raw data replaces
    // an interim index that had raw data)
    void
    reload_field_data(const FieldId field_id);

    void
    load_system_field_internal(
        FieldId field_id,
        FieldDataInfo& data,
        milvus::proto::common::LoadPriority load_priority);

    template <typename PK>
    void
    search_sorted_pk_range_impl(
        proto::plan::OpType op,
        const PK& target,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column,
        BitsetTypeView& bitset) const {
        const auto num_chunk = pk_column->num_chunks();
        if (num_chunk == 0) {
            return;
        }
        auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

        if (op == proto::plan::OpType::Equal) {
            // find first occurrence
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target, pk_column.get(), all_chunk_pins, 0);

            if (exact_match) {
                // find last occurrence
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(target,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    chunk_id,
                                                    in_chunk_offset);

                auto start_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
                auto end_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                               last_in_chunk_offset;

                bitset.set(start_idx, end_idx - start_idx + 1, true);
            }
        } else if (op == proto::plan::OpType::GreaterEqual ||
                   op == proto::plan::OpType::GreaterThan) {
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target, pk_column.get(), all_chunk_pins, 0);

            if (chunk_id != -1) {
                int64_t start_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
                if (exact_match && op == proto::plan::OpType::GreaterThan) {
                    auto [last_chunk_id, last_in_chunk_offset] =
                        this->find_last_pk_position<PK>(target,
                                                        pk_column.get(),
                                                        all_chunk_pins,
                                                        chunk_id,
                                                        in_chunk_offset);
                    start_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                                last_in_chunk_offset + 1;
                }
                if (start_idx < bitset.size()) {
                    bitset.set(start_idx, bitset.size() - start_idx, true);
                }
            }
        } else if (op == proto::plan::OpType::LessEqual ||
                   op == proto::plan::OpType::LessThan) {
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target, pk_column.get(), all_chunk_pins, 0);

            int64_t end_idx;
            if (chunk_id == -1) {
                end_idx = bitset.size();
            } else if (op == proto::plan::OpType::LessEqual && exact_match) {
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(target,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    chunk_id,
                                                    in_chunk_offset);
                end_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                          last_in_chunk_offset + 1;
            } else {
                end_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
            }

            if (end_idx > 0) {
                bitset.set(0, end_idx, true);
            }
        } else {
            ThrowInfo(ErrorCode::Unsupported,
                      fmt::format("unsupported op type {}", op));
        }
    }

    template <typename PK>
    void
    search_sorted_pk_binary_range_impl(
        const PK& lower_val,
        bool lower_inclusive,
        const PK& upper_val,
        bool upper_inclusive,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column,
        BitsetTypeView& bitset) const {
        const auto num_chunk = pk_column->num_chunks();
        if (num_chunk == 0) {
            return;
        }
        auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

        // Find the lower bound position (first value >= lower_val or > lower_val)
        auto [lower_chunk_id, lower_in_chunk_offset, lower_exact_match] =
            this->pk_lower_bound<PK>(
                lower_val, pk_column.get(), all_chunk_pins, 0);

        int64_t start_idx = 0;
        if (lower_chunk_id != -1) {
            start_idx = pk_column->GetNumRowsUntilChunk(lower_chunk_id) +
                        lower_in_chunk_offset;
            // If lower_inclusive is false and we found an exact match, skip all equal values
            if (!lower_inclusive && lower_exact_match) {
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(lower_val,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    lower_chunk_id,
                                                    lower_in_chunk_offset);
                start_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                            last_in_chunk_offset + 1;
            }
        } else {
            // lower_val is greater than all values, no results
            return;
        }

        // Find the upper bound position (first value >= upper_val or > upper_val)
        auto [upper_chunk_id, upper_in_chunk_offset, upper_exact_match] =
            this->pk_lower_bound<PK>(
                upper_val, pk_column.get(), all_chunk_pins, 0);

        int64_t end_idx = 0;
        if (upper_chunk_id == -1) {
            // upper_val is greater than all values, include all from start_idx to end
            end_idx = bitset.size();
        } else {
            // If upper_inclusive is true and we found an exact match, include all equal values
            if (upper_inclusive && upper_exact_match) {
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(upper_val,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    upper_chunk_id,
                                                    upper_in_chunk_offset);
                end_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                          last_in_chunk_offset + 1;
            } else {
                // upper_inclusive is false or no exact match
                // In both cases, end at the position of first value >= upper_val
                end_idx = pk_column->GetNumRowsUntilChunk(upper_chunk_id) +
                          upper_in_chunk_offset;
            }
        }

        // Set bits from start_idx to end_idx - 1
        if (start_idx < end_idx) {
            bitset.set(start_idx, end_idx - start_idx, true);
        }
    }

    template <typename PK>
    void
    search_pks_with_two_pointers_impl(
        BitsetTypeView& bitset,
        const std::vector<PkType>& pks,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column) const {
        // TODO: we should sort pks during plan generation
        std::vector<PK> sorted_pks;
        sorted_pks.reserve(pks.size());
        for (const auto& pk : pks) {
            sorted_pks.push_back(std::get<PK>(pk));
        }
        std::sort(sorted_pks.begin(), sorted_pks.end());

        auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

        size_t pk_idx = 0;
        int last_chunk_id = 0;

        while (pk_idx < sorted_pks.size()) {
            const auto& target_pk = sorted_pks[pk_idx];

            // find the first occurrence of target_pk
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target_pk, pk_column.get(), all_chunk_pins, last_chunk_id);

            if (chunk_id == -1) {
                // All remaining PKs are greater than all values in pk_column
                break;
            }

            if (exact_match) {
                // Found exact match, find the last occurrence
                auto [last_chunk_id_found, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(target_pk,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    chunk_id,
                                                    in_chunk_offset);

                // Mark all occurrences from first to last position using global indices
                auto start_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
                auto end_idx =
                    pk_column->GetNumRowsUntilChunk(last_chunk_id_found) +
                    last_in_chunk_offset;

                bitset.set(start_idx, end_idx - start_idx + 1, true);
                last_chunk_id = last_chunk_id_found;
            }

            while (pk_idx < sorted_pks.size() &&
                   sorted_pks[pk_idx] == target_pk) {
                pk_idx++;
            }
        }
    }

    // Binary search to find lower_bound of pk in pk_column starting from from_chunk_id
    // Returns: (chunk_id, in_chunk_offset, exists)
    //   - chunk_id: the chunk containing the first value >= pk
    //   - in_chunk_offset: offset of the first value >= pk in that chunk
    //   - exists: true if found an exact match (value == pk), false otherwise
    //   - If pk doesn't exist, returns the position of first value > pk with exists=false
    //   - If pk is greater than all values, returns {-1, -1, false}
    template <typename PK>
    std::tuple<int, int, bool>
    pk_lower_bound(const PK& pk,
                   const ChunkedColumnInterface* pk_column,
                   const std::vector<PinWrapper<Chunk*>>& all_chunk_pins,
                   int from_chunk_id = 0) const {
        const auto num_chunk = pk_column->num_chunks();

        if (from_chunk_id >= num_chunk) {
            return {-1, -1, false};  // Invalid starting chunk
        }

        using PKViewType = std::conditional_t<std::is_same_v<PK, int64_t>,
                                              int64_t,
                                              std::string_view>;

        auto get_val_view = [&](int chunk_id,
                                int in_chunk_offset) -> PKViewType {
            auto pw = all_chunk_pins[chunk_id];
            if constexpr (std::is_same_v<PK, int64_t>) {
                auto src =
                    reinterpret_cast<const int64_t*>(pw.get()->RawData());
                return src[in_chunk_offset];
            } else {
                auto string_chunk = static_cast<StringChunk*>(pw.get());
                return string_chunk->operator[](in_chunk_offset);
            }
        };

        // Binary search at chunk level to find the first chunk that might contain pk
        int left_chunk_id = from_chunk_id;
        int right_chunk_id = num_chunk - 1;
        int target_chunk_id = -1;

        while (left_chunk_id <= right_chunk_id) {
            int mid_chunk_id =
                left_chunk_id + (right_chunk_id - left_chunk_id) / 2;
            auto chunk_row_num = pk_column->chunk_row_nums(mid_chunk_id);

            PKViewType min_val = get_val_view(mid_chunk_id, 0);
            PKViewType max_val = get_val_view(mid_chunk_id, chunk_row_num - 1);

            if (pk >= min_val && pk <= max_val) {
                // pk might be in this chunk
                target_chunk_id = mid_chunk_id;
                break;
            } else if (pk < min_val) {
                // pk is before this chunk, could be in an earlier chunk
                target_chunk_id = mid_chunk_id;  // This chunk has values >= pk
                right_chunk_id = mid_chunk_id - 1;
            } else {
                // pk is after this chunk, search in later chunks
                left_chunk_id = mid_chunk_id + 1;
            }
        }

        // If no suitable chunk found, check if we need the first position after all chunks
        if (target_chunk_id == -1) {
            if (left_chunk_id >= num_chunk) {
                // pk is greater than all values
                return {-1, -1, false};
            }
            target_chunk_id = left_chunk_id;
        }

        // Binary search within the target chunk to find lower_bound position
        auto chunk_row_num = pk_column->chunk_row_nums(target_chunk_id);
        int left_offset = 0;
        int right_offset = chunk_row_num;

        while (left_offset < right_offset) {
            int mid_offset = left_offset + (right_offset - left_offset) / 2;
            PKViewType mid_val = get_val_view(target_chunk_id, mid_offset);

            if (mid_val < pk) {
                left_offset = mid_offset + 1;
            } else {
                right_offset = mid_offset;
            }
        }

        // Check if we found a valid position
        if (left_offset < chunk_row_num) {
            // Found position within current chunk
            PKViewType found_val = get_val_view(target_chunk_id, left_offset);
            bool exact_match = (found_val == pk);
            return {target_chunk_id, left_offset, exact_match};
        } else {
            // Position is beyond current chunk, try next chunk
            if (target_chunk_id + 1 < num_chunk) {
                // Next chunk exists, return its first position
                // Check if the first value in next chunk equals pk
                PKViewType next_val = get_val_view(target_chunk_id + 1, 0);
                bool exact_match = (next_val == pk);
                return {target_chunk_id + 1, 0, exact_match};
            } else {
                // No more chunks, pk is greater than all values
                return {-1, -1, false};
            }
        }
    }

    // Find the last occurrence position of pk starting from a known first occurrence
    // Parameters:
    //   - pk: the primary key to search for
    //   - pk_column: the primary key column
    //   - first_chunk_id: chunk id of the first occurrence (from pk_lower_bound)
    //   - first_in_chunk_offset: offset in chunk of the first occurrence (from pk_lower_bound)
    // Returns: (last_chunk_id, last_in_chunk_offset)
    //   - The position of the last occurrence of pk
    // Note: This function assumes pk exists and linearly scans forward.
    //       It's efficient when pk has few duplicates.
    template <typename PK>
    std::tuple<int, int>
    find_last_pk_position(const PK& pk,
                          const ChunkedColumnInterface* pk_column,
                          const std::vector<PinWrapper<Chunk*>>& all_chunk_pins,
                          int first_chunk_id,
                          int first_in_chunk_offset) const {
        const auto num_chunk = pk_column->num_chunks();

        using PKViewType = std::conditional_t<std::is_same_v<PK, int64_t>,
                                              int64_t,
                                              std::string_view>;

        auto get_val_view = [&](int chunk_id,
                                int in_chunk_offset) -> PKViewType {
            auto pw = all_chunk_pins[chunk_id];
            if constexpr (std::is_same_v<PK, int64_t>) {
                auto src =
                    reinterpret_cast<const int64_t*>(pw.get()->RawData());
                return src[in_chunk_offset];
            } else {
                auto string_chunk = static_cast<StringChunk*>(pw.get());
                return string_chunk->operator[](in_chunk_offset);
            }
        };

        int last_chunk_id = first_chunk_id;
        int last_offset = first_in_chunk_offset;

        // Linear scan forward in current chunk
        auto chunk_row_num = pk_column->chunk_row_nums(first_chunk_id);
        for (int offset = first_in_chunk_offset + 1; offset < chunk_row_num;
             offset++) {
            PKViewType curr_val = get_val_view(first_chunk_id, offset);
            if (curr_val == pk) {
                last_offset = offset;
            } else {
                // Found first value != pk, done
                return {last_chunk_id, last_offset};
            }
        }

        // Continue scanning in subsequent chunks
        for (int chunk_id = first_chunk_id + 1; chunk_id < num_chunk;
             chunk_id++) {
            auto curr_chunk_row_num = pk_column->chunk_row_nums(chunk_id);

            // Check first value in this chunk
            PKViewType first_val = get_val_view(chunk_id, 0);
            if (first_val != pk) {
                // This chunk doesn't contain pk anymore
                return {last_chunk_id, last_offset};
            }

            // Update last position and scan this chunk
            last_chunk_id = chunk_id;
            last_offset = 0;

            for (int offset = 1; offset < curr_chunk_row_num; offset++) {
                PKViewType curr_val = get_val_view(chunk_id, offset);
                if (curr_val == pk) {
                    last_offset = offset;
                } else {
                    // Found first value != pk
                    return {last_chunk_id, last_offset};
                }
            }
            // All values in this chunk equal pk, continue to next chunk
        }

        // Scanned all chunks, return last found position
        return {last_chunk_id, last_offset};
    }

    template <typename S, typename T = S>
    static void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        const void* src_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        T* dst_raw);

    template <typename S, typename T = S>
    static void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        ChunkedColumnInterface* field,
                        const int64_t* seg_offsets,
                        int64_t count,
                        T* dst_raw);

    static void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        int64_t element_sizeof,
                        ChunkedColumnInterface* field,
                        const int64_t* seg_offsets,
                        int64_t count,
                        void* dst_raw);

    template <typename S>
    static void
    bulk_subscript_ptr_impl(
        milvus::OpContext* op_ctx,
        ChunkedColumnInterface* field,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<std::string>* dst_raw);

    template <typename T>
    static void
    bulk_subscript_array_impl(milvus::OpContext* op_ctx,
                              ChunkedColumnInterface* column,
                              const int64_t* seg_offsets,
                              int64_t count,
                              google::protobuf::RepeatedPtrField<T>* dst);

    template <typename T>
    static void
    bulk_subscript_vector_array_impl(
        milvus::OpContext* op_ctx,
        const ChunkedColumnInterface* column,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<T>* dst);

    std::unique_ptr<DataArray>
    fill_with_empty(FieldId field_id, int64_t count) const;

    std::unique_ptr<DataArray>
    get_raw_data(milvus::OpContext* op_ctx,
                 FieldId field_id,
                 const FieldMeta& field_meta,
                 const int64_t* seg_offsets,
                 int64_t count) const;

    void
    update_row_count(int64_t row_count) {
        num_rows_ = row_count;
        deleted_record_.set_sealed_row_count(row_count);
    }

    void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp,
                         Timestamp collection_ttl) const override;

    void
    vector_search(SearchInfo& search_info,
                  const void* query_data,
                  const size_t* query_offsets,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  milvus::OpContext* op_context,
                  SearchResult& output) const override;

    void
    mask_with_delete(BitsetTypeView& bitset,
                     int64_t ins_barrier,
                     Timestamp timestamp) const override;

    bool
    is_system_field_ready() const {
        return system_ready_count_ == 1;
    }

    void
    search_ids(BitsetType& bitset, const IdArray& id_array) const override;

    void
    LoadVecIndex(const LoadIndexInfo& info);

    void
    LoadScalarIndex(const LoadIndexInfo& info);

    bool
    generate_interim_index(const FieldId field_id, int64_t num_rows);

    void
    fill_empty_field(const FieldMeta& field_meta);

    void
    init_timestamp_index(const std::vector<Timestamp>& timestamps,
                         size_t num_rows);

    void
    load_field_data_internal(const LoadFieldDataInfo& load_info);

    void
    load_column_group_data_internal(const LoadFieldDataInfo& load_info);

    void
    load_field_data_common(
        FieldId field_id,
        const std::shared_ptr<ChunkedColumnInterface>& column,
        size_t num_rows,
        DataType data_type,
        bool enable_mmap,
        bool is_proxy_column);

    std::shared_ptr<ChunkedColumnInterface>
    get_column(FieldId field_id) const {
        std::shared_ptr<ChunkedColumnInterface> res;
        fields_.withRLock([&](auto& fields) {
            auto it = fields.find(field_id);
            if (it != fields.end()) {
                res = it->second;
            }
        });
        return res;
    }

 private:
    // InsertRecord needs to pin pk column.
    friend class storagev1translator::InsertRecordTranslator;

    // mmap descriptor, used in chunk cache
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    // segment loading state
    BitsetType field_data_ready_bitset_;
    BitsetType index_ready_bitset_;
    BitsetType binlog_index_bitset_;
    std::atomic<int> system_ready_count_ = 0;

    // when index is ready (index_ready_bitset_/binlog_index_bitset_ is set to true), must also set index_has_raw_data_
    // to indicate whether the loaded index has raw data.
    std::unordered_map<FieldId, bool> index_has_raw_data_;

    // TODO: generate index for scalar
    std::optional<int64_t> num_rows_;

    // ngram indexings for json type
    folly::Synchronized<std::unordered_map<
        FieldId,
        std::unordered_map<std::string, index::CacheIndexBasePtr>>>
        ngram_indexings_;

    // fields that has ngram index
    folly::Synchronized<std::unordered_set<FieldId>> ngram_fields_;

    // scalar field index
    folly::Synchronized<std::unordered_map<FieldId, index::CacheIndexBasePtr>>
        scalar_indexings_;
    // vector field index
    SealedIndexingRecord vector_indexings_;

    // inserted fields data and row_ids, timestamps
    InsertRecord<true> insert_record_;

    // deleted pks
    mutable DeletedRecord<true> deleted_record_;

    LoadFieldDataInfo field_data_info_;

    SchemaPtr schema_;
    int64_t id_;
    mutable folly::Synchronized<
        std::unordered_map<FieldId, std::shared_ptr<ChunkedColumnInterface>>>
        fields_;
    std::unordered_set<FieldId> mmap_field_ids_;

    // only useful in binlog
    IndexMetaPtr col_index_meta_;
    SegcoreConfig segcore_config_;
    std::unordered_map<FieldId, std::unique_ptr<VecIndexConfig>>
        vec_binlog_config_;

    SegmentStats stats_{};

    // whether the segment is sorted by the pk
    // 1. will skip index loading for primary key field
    bool is_sorted_by_pk_ = false;
};

inline SegmentSealedUPtr
CreateSealedSegment(
    SchemaPtr schema,
    IndexMetaPtr index_meta = empty_index_meta,
    int64_t segment_id = 0,
    const SegcoreConfig& segcore_config = SegcoreConfig::default_config(),
    bool is_sorted_by_pk = false) {
    return std::make_unique<ChunkedSegmentSealedImpl>(
        schema, index_meta, segcore_config, segment_id, is_sorted_by_pk);
}
}  // namespace milvus::segcore
