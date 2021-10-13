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
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

#include <shared_mutex>
#include <knowhere/index/vector_index/VecIndex.h>
#include <query/PlanNode.h>

#include "AckResponder.h"
#include "SealedIndexingRecord.h"
#include "ConcurrentVector.h"
#include "segcore/SegmentGrowing.h"
#include "query/deprecated/GeneralQuery.h"
#include "utils/Status.h"
#include "segcore/DeletedRecord.h"
#include "exceptions/EasyAssert.h"
#include "FieldIndexing.h"
#include "InsertRecord.h"
#include <utility>
#include <memory>
#include <string>
#include <vector>
#include <deque>

namespace milvus::segcore {

class SegmentGrowingImpl : public SegmentGrowing {
 public:
    int64_t
    PreInsert(int64_t size) override;

    Status
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const RowBasedRawData& values) override;

    void
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const ColumnBasedRawData& values) override;

    int64_t
    PreDelete(int64_t size) override;

    // TODO: add id into delete log, possibly bitmap
    Status
    Delete(int64_t reserverd_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) override;

    int64_t
    GetMemoryUsageInBytes() const override;

    std::string
    debug() const override;

 public:
    const InsertRecord&
    get_insert_record() const {
        return record_;
    }

    const IndexingRecord&
    get_indexing_record() const {
        return indexing_record_;
    }

    const DeletedRecord&
    get_deleted_record() const {
        return deleted_record_;
    }

    const SealedIndexingRecord&
    get_sealed_indexing_record() const {
        return sealed_indexing_record_;
    }

    const Schema&
    get_schema() const override {
        return *schema_;
    }

    // return count of index that has index, i.e., [0, num_chunk_index) have built index
    int64_t
    num_chunk_index(FieldOffset field_offset) const final {
        return indexing_record_.get_finished_ack();
    }

    // deprecated
    const knowhere::Index*
    chunk_index_impl(FieldOffset field_offset, int64_t chunk_id) const final {
        return indexing_record_.get_field_indexing(field_offset).get_chunk_indexing(chunk_id);
    }

    int64_t
    size_per_chunk() const final {
        return segcore_config_.get_chunk_rows();
    }

 public:
    // only for debug
    void
    disable_small_index() override {
        enable_small_index_ = false;
    }

    ssize_t
    get_row_count() const override {
        return record_.ack_responder_.GetAck();
    }

    ssize_t
    get_deleted_count() const override {
        return 0;
    }

    int64_t
    get_active_count(Timestamp ts) const override;

    // for scalar vectors
    template <typename T>
    void
    bulk_subscript_impl(
        const VectorBase& vec_raw, const int64_t* seg_offsets, int64_t count, T default_value, void* output_raw) const;

    template <typename T>
    void
    bulk_subscript_impl(int64_t element_sizeof,
                        const VectorBase& vec_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        void* output_raw) const;

    void
    bulk_subscript(SystemFieldType system_type, const int64_t* seg_offsets, int64_t count, void* output) const override;

    void
    bulk_subscript(FieldOffset field_offset, const int64_t* seg_offsets, int64_t count, void* output) const override;

 public:
    friend std::unique_ptr<SegmentGrowing>
    CreateGrowingSegment(SchemaPtr schema, const SegcoreConfig& segcore_config);

    explicit SegmentGrowingImpl(SchemaPtr schema, const SegcoreConfig& segcore_config)
        : segcore_config_(segcore_config),
          schema_(std::move(schema)),
          record_(*schema_, segcore_config.get_chunk_rows()),
          indexing_record_(*schema_, segcore_config_) {
    }

    void
    mask_with_timestamps(boost::dynamic_bitset<>& bitset_chunk, Timestamp timestamp) const override;

    void
    vector_search(int64_t vec_count,
                  query::SearchInfo search_info,
                  const void* query_data,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  SearchResult& output) const override;

 public:
    std::shared_ptr<DeletedRecord::TmpBitmap>
    get_deleted_bitmap(int64_t del_barrier, Timestamp query_timestamp, int64_t insert_barrier, bool force = false);

    const BitsetView
    get_filtered_bitmap(BitsetView& bitset, int64_t ins_barrier, Timestamp timestamp);

    std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
    search_ids(const IdArray& id_array, Timestamp timestamp) const override;

    std::vector<SegOffset>
    search_ids(const boost::dynamic_bitset<>& view, Timestamp timestamp) const override;

 protected:
    int64_t
    num_chunk() const override;

    SpanBase
    chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const override;

    void
    check_search(const query::Plan* plan) const override {
        Assert(plan);
    }

 private:
    void
    do_insert(int64_t reserved_begin,
              int64_t size,
              const idx_t* row_ids,
              const Timestamp* timestamps,
              const std::vector<aligned_vector<uint8_t>>& columns_data);

 private:
    SegcoreConfig segcore_config_;
    SchemaPtr schema_;

    InsertRecord record_;
    DeletedRecord deleted_record_;
    IndexingRecord indexing_record_;
    SealedIndexingRecord sealed_indexing_record_;

    tbb::concurrent_unordered_multimap<idx_t, int64_t> uid2offset_;

 private:
    bool enable_small_index_ = true;
};

}  // namespace milvus::segcore
