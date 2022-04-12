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

#include <deque>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <tbb/concurrent_priority_queue.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

#include "ConcurrentVector.h"
#include "DeletedRecord.h"
#include "ScalarIndex.h"
#include "SealedIndexingRecord.h"
#include "SegmentSealed.h"
#include "TimestampIndex.h"

namespace milvus::segcore {

class SegmentSealedImpl : public SegmentSealed {
 public:
    explicit SegmentSealedImpl(SchemaPtr schema, int64_t segment_id);
    void
    LoadIndex(const LoadIndexInfo& info) override;
    void
    LoadFieldData(const LoadFieldDataInfo& info) override;
    void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) override;
    void
    LoadSegmentMeta(const milvus::proto::segcore::LoadSegmentMeta& segment_meta) override;
    void
    DropIndex(const FieldId field_id) override;
    void
    DropFieldData(const FieldId field_id) override;
    bool
    HasIndex(FieldId field_id) const override;
    bool
    HasFieldData(FieldId field_id) const override;

 public:
    int64_t
    GetMemoryUsageInBytes() const override;

    int64_t
    get_row_count() const override;

    const Schema&
    get_schema() const override;

 public:
    int64_t
    num_chunk_index(FieldOffset field_offset) const override;

    int64_t
    num_chunk() const override;

    // return size_per_chunk for each chunk, renaming against confusion
    int64_t
    size_per_chunk() const override;

    std::string
    debug() const override;

    int64_t
    PreDelete(int64_t size) override;

    Status
    Delete(int64_t reserved_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) override;

 protected:
    // blob and row_count
    SpanBase
    chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const override;

    const knowhere::Index*
    chunk_index_impl(FieldOffset field_offset, int64_t chunk_id) const override;

    // Calculate: output[i] = Vec[seg_offset[i]],
    // where Vec is determined from field_offset
    void
    bulk_subscript(SystemFieldType system_type, const int64_t* seg_offsets, int64_t count, void* output) const override;

    // Calculate: output[i] = Vec[seg_offset[i]]
    // where Vec is determined from field_offset
    void
    bulk_subscript(FieldOffset field_offset, const int64_t* seg_offsets, int64_t count, void* output) const override;

    void
    check_search(const query::Plan* plan) const override;

    int64_t
    get_active_count(Timestamp ts) const override;

    std::shared_ptr<DeletedRecord::TmpBitmap>
    get_deleted_bitmap(int64_t del_barrier,
                       Timestamp query_timestamp,
                       int64_t insert_barrier,
                       bool force = false) const;

 private:
    template <typename T>
    static void
    bulk_subscript_impl(const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw);

    static void
    bulk_subscript_impl(
        int64_t element_sizeof, const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw);

    void
    update_row_count(int64_t row_count) {
        if (row_count_opt_.has_value()) {
            AssertInfo(row_count_opt_.value() == row_count, "load data has different row count from other columns");
        } else {
            row_count_opt_ = row_count;
        }
    }

    void
    mask_with_timestamps(BitsetType& bitset_chunk, Timestamp timestamp) const override;

    void
    vector_search(int64_t vec_count,
                  query::SearchInfo search_info,
                  const void* query_data,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  SearchResult& output) const override;

    void
    mask_with_delete(BitsetType& bitset, int64_t ins_barrier, Timestamp timestamp) const override;

    bool
    is_system_field_ready() const {
        return system_ready_count_ == 2;
    }

    const DeletedRecord&
    get_deleted_record() const {
        return deleted_record_;
    }

    std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
    search_ids(const IdArray& id_array, Timestamp timestamp) const override;

    std::vector<SegOffset>
    search_ids(const BitsetView& view, Timestamp timestamp) const override;

    std::vector<SegOffset>
    search_ids(const BitsetType& view, Timestamp timestamp) const override;

    //    virtual void
    //    build_index_if_primary_key(FieldId field_id);

 private:
    // segment loading state
    BitsetType field_data_ready_bitset_;
    BitsetType vecindex_ready_bitset_;
    std::atomic<int> system_ready_count_ = 0;
    // segment datas

    // TODO: generate index for scalar
    std::optional<int64_t> row_count_opt_;

    // TODO: use protobuf format
    // TODO: remove duplicated indexing
    std::vector<std::unique_ptr<knowhere::Index>> scalar_indexings_;
    std::unique_ptr<ScalarIndexBase> primary_key_index_;

    std::vector<aligned_vector<char>> fields_data_;
    mutable DeletedRecord deleted_record_;

    SealedIndexingRecord vecindexs_;
    aligned_vector<idx_t> row_ids_;
    aligned_vector<Timestamp> timestamps_;
    TimestampIndex timestamp_index_;
    SchemaPtr schema_;
    int64_t id_;
};

inline SegmentSealedPtr
CreateSealedSegment(SchemaPtr schema, int64_t segment_id = -1) {
    return std::make_unique<SegmentSealedImpl>(schema, segment_id);
}

}  // namespace milvus::segcore
