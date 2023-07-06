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
#include <memory>
#include <shared_mutex>
#include <string>
#include <tbb/concurrent_priority_queue.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <vector>
#include <utility>

#include "AckResponder.h"
#include "ConcurrentVector.h"
#include "DeletedRecord.h"
#include "FieldIndexing.h"
#include "InsertRecord.h"
#include "SealedIndexingRecord.h"
#include "SegmentGrowing.h"

#include "exceptions/EasyAssert.h"
#include "query/PlanNode.h"
#include "query/deprecated/GeneralQuery.h"
#include "utils/Status.h"
#include "common/IndexMeta.h"

namespace milvus::segcore {

class SegmentGrowingImpl : public SegmentGrowing {
 public:
    int64_t
    PreInsert(int64_t size) override;

    void
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const InsertData* insert_data) override;

    // TODO: add id into delete log, possibly bitmap
    Status
    Delete(int64_t reserved_offset,
           int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) override;

    int64_t
    GetMemoryUsageInBytes() const override;

    void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) override;

    void
    LoadFieldData(const LoadFieldDataInfo& info) override;

    std::string
    debug() const override;

    int64_t
    get_segment_id() const override {
        return id_;
    }

 public:
    const InsertRecord<>&
    get_insert_record() const {
        return insert_record_;
    }

    const IndexingRecord&
    get_indexing_record() const {
        return indexing_record_;
    }

    const DeletedRecord&
    get_deleted_record() const {
        return deleted_record_;
    }

    std::shared_mutex&
    get_chunk_mutex() const {
        return chunk_mutex_;
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
    num_chunk_index(FieldId field_id) const final {
        return indexing_record_.get_finished_ack();
    }

    // count of chunk that has raw data
    int64_t
    num_chunk_data(FieldId field_id) const final {
        auto size = get_insert_record().ack_responder_.GetAck();
        return upper_div(size, segcore_config_.get_chunk_rows());
    }

    // deprecated
    const index::IndexBase*
    chunk_index_impl(FieldId field_id, int64_t chunk_id) const final {
        return indexing_record_.get_field_indexing(field_id).get_chunk_indexing(
            chunk_id);
    }

    int64_t
    size_per_chunk() const final {
        return segcore_config_.get_chunk_rows();
    }

    void
    try_remove_chunks(FieldId fieldId);

 public:
    int64_t
    get_row_count() const override {
        return insert_record_.ack_responder_.GetAck();
    }

    int64_t
    get_deleted_count() const override {
        return deleted_record_.size();
    }

    int64_t
    get_active_count(Timestamp ts) const override;

    // for scalar vectors
    template <typename S, typename T = S>
    void
    bulk_subscript_impl(const VectorBase& vec_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        void* output_raw) const;

    template <typename T>
    void
    bulk_subscript_impl(FieldId field_id,
                        int64_t element_sizeof,
                        const VectorBase& vec_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        void* output_raw) const;

    void
    bulk_subscript(SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const override;

    std::unique_ptr<DataArray>
    bulk_subscript(FieldId field_id,
                   const int64_t* seg_offsets,
                   int64_t count) const override;

 public:
    friend std::unique_ptr<SegmentGrowing>
    CreateGrowingSegment(SchemaPtr schema,
                         const SegcoreConfig& segcore_config,
                         int64_t segment_id);

    explicit SegmentGrowingImpl(SchemaPtr schema,
                                IndexMetaPtr indexMeta,
                                const SegcoreConfig& segcore_config,
                                int64_t segment_id)
        : segcore_config_(segcore_config),
          schema_(std::move(schema)),
          index_meta_(indexMeta),
          insert_record_(*schema_, segcore_config.get_chunk_rows()),
          indexing_record_(*schema_, index_meta_, segcore_config_),
          id_(segment_id) {
    }

    void
    mask_with_timestamps(BitsetType& bitset_chunk,
                         Timestamp timestamp) const override;

    void
    vector_search(SearchInfo& search_info,
                  const void* query_data,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  SearchResult& output) const override;

 public:
    void
    mask_with_delete(BitsetType& bitset,
                     int64_t ins_barrier,
                     Timestamp timestamp) const override;

    std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
    search_ids(const IdArray& id_array, Timestamp timestamp) const override;

    std::vector<SegOffset>
    search_ids(const BitsetType& view, Timestamp timestamp) const override;

    std::vector<SegOffset>
    search_ids(const BitsetView& view, Timestamp timestamp) const override;

    std::vector<SegOffset>
    search_ids(const BitsetView& view,
               const std::vector<int64_t>& offsets,
               Timestamp timestamp) const override;

    bool
    HasIndex(FieldId field_id) const override {
        return true;
    }

    bool
    HasFieldData(FieldId field_id) const override {
        return true;
    }

    bool
    HasRawData(int64_t field_id) const override {
        //growing index hold raw data when
        // 1. growing index enabled and it holds raw data
        // 2. growing index disabled then raw data held by chunk
        if (indexing_record_.is_in(FieldId(field_id))) {
            return indexing_record_.HasRawData(FieldId(field_id));
        }
        return true;
    }

 protected:
    int64_t
    num_chunk() const override;

    SpanBase
    chunk_data_impl(FieldId field_id, int64_t chunk_id) const override;

    void
    check_search(const query::Plan* plan) const override {
        Assert(plan);
    }

 private:
    SegcoreConfig segcore_config_;
    SchemaPtr schema_;
    IndexMetaPtr index_meta_;

    // small indexes for every chunk
    IndexingRecord indexing_record_;
    SealedIndexingRecord sealed_indexing_record_;  // not used

    // inserted fields data and row_ids, timestamps
    InsertRecord<false> insert_record_;

    mutable std::shared_mutex chunk_mutex_;

    // deleted pks
    mutable DeletedRecord deleted_record_;

    int64_t id_;
};

const static IndexMetaPtr empty_index_meta =
    std::make_shared<CollectionIndexMeta>(1024,
                                          std::map<FieldId, FieldIndexMeta>());

inline SegmentGrowingPtr
CreateGrowingSegment(
    SchemaPtr schema,
    IndexMetaPtr indexMeta,
    int64_t segment_id = -1,
    const SegcoreConfig& conf = SegcoreConfig::default_config()) {
    return std::make_unique<SegmentGrowingImpl>(
        schema, indexMeta, conf, segment_id);
}

}  // namespace milvus::segcore
