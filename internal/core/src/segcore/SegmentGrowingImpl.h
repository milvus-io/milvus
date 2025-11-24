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

#include "cachinglayer/CacheSlot.h"
#include "AckResponder.h"
#include "ConcurrentVector.h"
#include "DeletedRecord.h"
#include "FieldIndexing.h"
#include "InsertRecord.h"
#include "SealedIndexingRecord.h"
#include "SegmentGrowing.h"
#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/Types.h"
#include "query/PlanNode.h"
#include "common/GeometryCache.h"

namespace milvus::segcore {

using namespace milvus::cachinglayer;

class SegmentGrowingImpl : public SegmentGrowing {
 public:
    int64_t
    PreInsert(int64_t size) override;

    void
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           InsertRecordProto* insert_record_proto) override;

    bool
    Contain(const PkType& pk) const override {
        return insert_record_.contain(pk);
    }

    // TODO: add id into delete log, possibly bitmap
    SegcoreError
    Delete(int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) override;

    void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) override;

    void
    LoadFieldData(const LoadFieldDataInfo& info) override;

    int64_t
    get_segment_id() const override {
        return id_;
    }

    bool
    is_nullable(FieldId field_id) const override {
        AssertInfo(insert_record_.is_data_exist(field_id),
                   "Cannot find field_data with field_id: " +
                       std::to_string(field_id.get()));
        return insert_record_.is_valid_data_exist(field_id);
    };

    void
    CreateTextIndex(FieldId field_id) override;

    void
    load_field_data_internal(const LoadFieldDataInfo& load_info);

    void
    load_column_group_data_internal(const LoadFieldDataInfo& load_info);

    void
    load_field_data_common(FieldId field_id,
                           size_t reserved_offset,
                           const std::vector<FieldDataPtr>& field_data,
                           FieldId primary_field_id,
                           size_t num_rows);

    void
    Reopen(SchemaPtr sch) override;

    void
    LazyCheckSchema(SchemaPtr sch) override;

    void
    FinishLoad() override;

    void
    Load(milvus::tracer::TraceContext& trace_ctx) override;

 private:
    // Build geometry cache for inserted data
    void
    BuildGeometryCacheForInsert(FieldId field_id,
                                const DataArray* data_array,
                                int64_t num_rows);

    // Build geometry cache for loaded field data
    void
    BuildGeometryCacheForLoad(FieldId field_id,
                              const std::vector<FieldDataPtr>& field_data);

 public:
    const InsertRecord<false>&
    get_insert_record() const {
        return insert_record_;
    }

    const IndexingRecord&
    get_indexing_record() const {
        return indexing_record_;
    }

    std::shared_mutex&
    get_chunk_mutex() const {
        return chunk_mutex_;
    }

    const Schema&
    get_schema() const override {
        return *schema_;
    }

    // return count of index that has index, i.e., [0, num_chunk_index) have built index
    int64_t
    num_chunk_index(FieldId field_id) const {
        return indexing_record_.get_finished_ack();
    }

    // count of chunk that has raw data
    int64_t
    num_chunk_data(FieldId field_id) const final {
        auto size = get_insert_record().ack_responder_.GetAck();
        return upper_div(size, segcore_config_.get_chunk_rows());
    }

    // deprecated
    PinWrapper<const index::IndexBase*>
    chunk_index_impl(FieldId field_id, int64_t chunk_id) const {
        return PinWrapper<const index::IndexBase*>(
            indexing_record_.get_field_indexing(field_id)
                .get_chunk_indexing(chunk_id)
                .get());
    }

    int64_t
    size_per_chunk() const final {
        return segcore_config_.get_chunk_rows();
    }

    virtual int64_t
    chunk_size(FieldId field_id, int64_t chunk_id) const final {
        return segcore_config_.get_chunk_rows();
    }

    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field_id, int64_t offset) const override {
        auto size_per_chunk = segcore_config_.get_chunk_rows();
        return {offset / size_per_chunk, offset % size_per_chunk};
    }

    int64_t
    num_rows_until_chunk(FieldId field_id, int64_t chunk_id) const override {
        return chunk_id * segcore_config_.get_chunk_rows();
    }

    void
    try_remove_chunks(FieldId fieldId);

    void
    search_batch_pks(
        const std::vector<PkType>& pks,
        const Timestamp* timestamps,
        bool include_same_ts,
        const std::function<void(const SegOffset offset, const Timestamp ts)>&
            callback) const;

 public:
    size_t
    GetMemoryUsageInBytes() const override {
        return stats_.mem_size.load() + deleted_record_.mem_size();
    }

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
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        const VectorBase* vec_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        T* output) const;

    template <typename S>
    void
    bulk_subscript_ptr_impl(
        milvus::OpContext* op_ctx,
        const VectorBase* vec_raw,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<std::string>* dst) const;

    // for scalar array vectors
    template <typename T>
    void
    bulk_subscript_array_impl(milvus::OpContext* op_ctx,
                              const VectorBase& vec_raw,
                              const int64_t* seg_offsets,
                              int64_t count,
                              google::protobuf::RepeatedPtrField<T>* dst) const;

    // for vector array vectors
    template <typename T>
    void
    bulk_subscript_vector_array_impl(
        milvus::OpContext* op_ctx,
        const VectorBase& vec_raw,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<T>* dst) const;

    template <typename T>
    void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        FieldId field_id,
                        int64_t element_sizeof,
                        const VectorBase* vec_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        void* output_raw) const;

    void
    bulk_subscript_sparse_float_vector_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        const ConcurrentVector<SparseFloatVector>* vec_raw,
        const int64_t* seg_offsets,
        int64_t count,
        milvus::proto::schema::SparseFloatArray* output) const;

    void
    bulk_subscript(milvus::OpContext* op_ctx,
                   SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const override;

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

    virtual void
    BulkGetJsonData(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    std::function<void(milvus::Json, size_t, bool)> fn,
                    const int64_t* offsets,
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
        : mmap_descriptor_(storage::MmapManager::GetInstance()
                               .GetMmapChunkManager()
                               ->Register()),
          segcore_config_(segcore_config),
          schema_(std::move(schema)),
          index_meta_(indexMeta),
          insert_record_(
              *schema_, segcore_config.get_chunk_rows(), mmap_descriptor_),
          indexing_record_(
              *schema_, index_meta_, segcore_config_, &insert_record_),
          id_(segment_id),
          deleted_record_(
              &insert_record_,
              [this](const std::vector<PkType>& pks,
                     const Timestamp* timestamps,
                     std::function<void(const SegOffset offset,
                                        const Timestamp ts)> callback) {
                  this->search_batch_pks(pks, timestamps, false, callback);
              },
              segment_id) {
        this->CreateTextIndexes();
    }

    ~SegmentGrowingImpl() {
        // Clean up geometry cache for all fields in this segment
        auto& cache_manager =
            milvus::exec::SimpleGeometryCacheManager::Instance();
        cache_manager.RemoveSegmentCaches(ctx_, get_segment_id());

        if (ctx_) {
            GEOS_finish_r(ctx_);
            ctx_ = nullptr;
        }

        // Original mmap cleanup logic
        if (mmap_descriptor_ != nullptr) {
            auto mcm =
                storage::MmapManager::GetInstance().GetMmapChunkManager();
            mcm->UnRegister(mmap_descriptor_);
        }
    }

    void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp,
                         Timestamp ttl = 0) const override;

    void
    vector_search(SearchInfo& search_info,
                  const void* query_data,
                  const size_t* query_offsets,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  milvus::OpContext* op_context,
                  SearchResult& output) const override;

    DataType
    GetFieldDataType(FieldId fieldId) const override;

 public:
    void
    mask_with_delete(BitsetTypeView& bitset,
                     int64_t ins_barrier,
                     Timestamp timestamp) const override;

    void
    search_ids(BitsetType& bitset, const IdArray& id_array) const override;

    bool
    HasIndex(FieldId field_id) const {
        auto& field_meta = schema_->operator[](field_id);
        if ((IsVectorDataType(field_meta.get_data_type()) ||
             IsGeometryType(field_meta.get_data_type())) &&
            indexing_record_.SyncDataWithIndex(field_id)) {
            return true;
        }

        return false;
    }

    std::vector<PinWrapper<const index::IndexBase*>>
    PinIndex(milvus::OpContext* op_ctx,
             FieldId field_id,
             bool include_ngram = false) const override {
        if (!HasIndex(field_id)) {
            return {};
        }

        auto& field_meta = schema_->operator[](field_id);

        // For geometry fields, return segment-level index (RTree doesn't use chunks)
        if (IsGeometryType(field_meta.get_data_type())) {
            auto segment_index = indexing_record_.get_field_indexing(field_id)
                                     .get_segment_indexing();
            if (segment_index.get() != nullptr) {
                // Convert from PinWrapper<index::IndexBase*> to PinWrapper<const index::IndexBase*>
                return {
                    PinWrapper<const index::IndexBase*>(segment_index.get())};
            } else {
                return {};
            }
        }

        // For vector fields, return chunk-level indexes
        auto num_chunk = num_chunk_index(field_id);
        std::vector<PinWrapper<const index::IndexBase*>> indexes;
        for (int64_t i = 0; i < num_chunk; i++) {
            indexes.push_back(chunk_index_impl(field_id, i));
        }
        return indexes;
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
        // 3. growing index enabled and it not holds raw data, then raw data held by chunk
        if (indexing_record_.is_in(FieldId(field_id))) {
            if (indexing_record_.HasRawData(FieldId(field_id))) {
                // 1. growing index enabled and it holds raw data
                return true;
            } else {
                // 3. growing index enabled and it not holds raw data, then raw data held by chunk
                return insert_record_.get_data_base(FieldId(field_id))
                           ->num_chunk() > 0;
            }
        }
        // 2. growing index disabled then raw data held by chunk
        return true;
    }

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first(int64_t limit, const BitsetType& bitset) const override {
        return insert_record_.pk2offset_->find_first(limit, bitset);
    }

    bool
    is_mmap_field(FieldId id) const override {
        return false;
    }

    void
    pk_range(milvus::OpContext* op_ctx,
             proto::plan::OpType op,
             const PkType& pk,
             BitsetTypeView& bitset) const override {
        insert_record_.search_pk_range(pk, op, bitset);
    }

    bool
    is_field_exist(FieldId field_id) const override {
        return schema_->get_fields().find(field_id) !=
               schema_->get_fields().end();
    }

    void
    LoadJsonStats(FieldId field_id,
                  index::CacheJsonKeyStatsPtr cache_slot) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "LoadJsonStats not implemented for SegmentGrowingImpl");
    }

    PinWrapper<index::JsonKeyStats*>
    GetJsonStats(milvus::OpContext* op_ctx, FieldId field_id) const override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "GetJsonStats not implemented for SegmentGrowingImpl");
    }

    void
    RemoveJsonStats(FieldId field_id) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "RemoveJsonStats not implemented for SegmentGrowingImpl");
    }

 protected:
    int64_t
    num_chunk(FieldId field_id) const override;

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

    void
    check_search(const query::Plan* plan) const override {
        Assert(plan);
    }

    const ConcurrentVector<Timestamp>&
    get_timestamps() const override {
        return insert_record_.timestamps_;
    }

    void
    fill_empty_field(const FieldMeta& field_meta);

 private:
    void
    AddTexts(FieldId field_id,
             const std::string* texts,
             const bool* texts_valid_data,
             size_t n,
             int64_t offset_begin);

    void
    CreateTextIndexes();

 private:
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    SegcoreConfig segcore_config_;
    SchemaPtr schema_;
    IndexMetaPtr index_meta_;

    // inserted fields data and row_ids, timestamps
    InsertRecord<false> insert_record_;

    mutable std::shared_mutex chunk_mutex_;

    // small indexes for every chunk
    IndexingRecord indexing_record_;

    // deleted pks
    mutable DeletedRecord<false> deleted_record_;

    int64_t id_;

    SegmentStats stats_{};
};

inline SegmentGrowingPtr
CreateGrowingSegment(
    SchemaPtr schema,
    IndexMetaPtr indexMeta,
    int64_t segment_id = 0,
    const SegcoreConfig& conf = SegcoreConfig::default_config()) {
    return std::make_unique<SegmentGrowingImpl>(
        schema, indexMeta, conf, segment_id);
}

}  // namespace milvus::segcore
