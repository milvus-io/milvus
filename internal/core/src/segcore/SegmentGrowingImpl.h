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
#include "utils/EasyAssert.h"
#include "FieldIndexing.h"
#include "InsertRecord.h"
#include <utility>
#include <memory>

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

    int64_t
    PreDelete(int64_t size) override;

    // TODO: add id into delete log, possibly bitmap
    Status
    Delete(int64_t reserverd_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) override;

    // stop receive insert requests
    // will move data to immutable vector or something
    Status
    Close() override;

    int64_t
    GetMemoryUsageInBytes() const override;

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

    const knowhere::Index*
    chunk_index_impl(FieldOffset field_offset, int64_t chunk_id) const final {
        return indexing_record_.get_field_indexing(field_offset).get_chunk_indexing(chunk_id);
    }

    int64_t
    size_per_chunk() const final {
        return size_per_chunk_;
    }

 public:
    ssize_t
    get_row_count() const override {
        return record_.ack_responder_.GetAck();
    }

    SegmentState
    get_state() const override {
        return state_.load(std::memory_order_relaxed);
    }

    ssize_t
    get_deleted_count() const override {
        return 0;
    }

    template <typename T>
    void
    bulk_subscript_impl(const VectorBase& vec_raw, const int64_t* seg_offsets, int64_t count, void* output_raw) const {
        static_assert(IsScalar<T>);
        auto vec_ptr = dynamic_cast<const ConcurrentVector<T>*>(&vec_raw);
        Assert(vec_ptr);
        auto& vec = *vec_ptr;
        auto output = reinterpret_cast<T*>(output_raw);
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            output[i] = offset == -1 ? -1 : vec[offset];
        }
    }

    void
    bulk_subscript(SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const override {
        switch (system_type) {
            case SystemFieldType::Timestamp:
                PanicInfo("timestamp unsupported");
            case SystemFieldType::RowId:
                bulk_subscript_impl<int64_t>(this->record_.uids_, seg_offsets, count, output);
                break;
            default:
                PanicInfo("unknown subscript fields");
        }
    }

    void
    bulk_subscript(FieldOffset field_offset, const int64_t* seg_offsets, int64_t count, void* output) const override {
        // TODO: support more types
        auto vec_ptr = record_.get_field_data_base(field_offset);
        auto data_type = schema_->operator[](field_offset).get_data_type();
        Assert(data_type == DataType::INT64);
        bulk_subscript_impl<int64_t>(*vec_ptr, seg_offsets, count, output);
    }

    int64_t
    num_chunk() const override;

    Status
    LoadIndexing(const LoadIndexInfo& info) override;

 public:
    friend std::unique_ptr<SegmentGrowing>
    CreateGrowingSegment(SchemaPtr schema, int64_t size_per_chunk);

    explicit SegmentGrowingImpl(SchemaPtr schema, int64_t size_per_chunk)
        : size_per_chunk_(size_per_chunk),
          schema_(std::move(schema)),
          record_(*schema_, size_per_chunk),
          indexing_record_(*schema_, size_per_chunk) {
    }

    void
    vector_search(int64_t vec_count,
                  query::QueryInfo query_info,
                  const void* query_data,
                  int64_t query_count,
                  const BitsetView& bitset,
                  QueryResult& output) const override;

 public:
    std::shared_ptr<DeletedRecord::TmpBitmap>
    get_deleted_bitmap(int64_t del_barrier, Timestamp query_timestamp, int64_t insert_barrier, bool force = false);

 protected:
    SpanBase
    chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const override;

 private:
    int64_t size_per_chunk_;
    SchemaPtr schema_;
    std::atomic<SegmentState> state_ = SegmentState::Open;

    InsertRecord record_;
    DeletedRecord deleted_record_;
    IndexingRecord indexing_record_;
    SealedIndexingRecord sealed_indexing_record_;

    tbb::concurrent_unordered_multimap<idx_t, int64_t> uid2offset_;
};

}  // namespace milvus::segcore
