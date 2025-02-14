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
#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/Types.h"
#include "query/PlanNode.h"

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
           const InsertRecordProto* insert_record_proto) override;

    bool
    Contain(const PkType& pk) const override {
        return insert_record_.contain(pk);
    }

    // TODO: add id into delete log, possibly bitmap
    SegcoreError
    Delete(int64_t reserved_offset,
           int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) override;

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

    bool
    is_nullable(FieldId field_id) const override {
        AssertInfo(insert_record_.is_data_exist(field_id),
                   "Cannot find field_data with field_id: " +
                       std::to_string(field_id.get()));
        return insert_record_.is_valid_data_exist(field_id);
    };

    void
    CreateTextIndex(FieldId field_id) override;

 public:
    const InsertRecord<>&
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
    bulk_subscript_impl(const VectorBase* vec_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        T* output) const;

    template <typename S, typename T = S>
    void
    bulk_subscript_ptr_impl(const VectorBase* vec_raw,
                            const int64_t* seg_offsets,
                            int64_t count,
                            google::protobuf::RepeatedPtrField<T>* dst) const;

    // for scalar array vectors
    template <typename T>
    void
    bulk_subscript_array_impl(const VectorBase& vec_raw,
                              const int64_t* seg_offsets,
                              int64_t count,
                              google::protobuf::RepeatedPtrField<T>* dst) const;

    template <typename T>
    void
    bulk_subscript_impl(FieldId field_id,
                        int64_t element_sizeof,
                        const VectorBase* vec_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        void* output_raw) const;

    void
    bulk_subscript_sparse_float_vector_impl(
        FieldId field_id,
        const ConcurrentVector<SparseFloatVector>* vec_raw,
        const int64_t* seg_offsets,
        int64_t count,
        milvus::proto::schema::SparseFloatArray* output) const;

    void
    bulk_subscript(SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const override;

    std::unique_ptr<DataArray>
    bulk_subscript(FieldId field_id,
                   const int64_t* seg_offsets,
                   int64_t count) const override;

    std::unique_ptr<DataArray>
    bulk_subscript(
        FieldId field_id,
        const int64_t* seg_offsets,
        int64_t count,
        const std::vector<std::string>& dynamic_field_names) const override;

    virtual std::pair<std::string_view, bool>
    GetJsonData(FieldId field_id, size_t offset) const override;

 public:
    friend std::unique_ptr<SegmentGrowing>
    CreateGrowingSegment(SchemaPtr schema,
                         const SegcoreConfig& segcore_config,
                         int64_t segment_id);

    explicit SegmentGrowingImpl(SchemaPtr schema,
                                IndexMetaPtr indexMeta,
                                const SegcoreConfig& segcore_config,
                                int64_t segment_id)
        : mmap_descriptor_(
              storage::MmapChunkDescriptorPtr(new storage::MmapChunkDescriptor(
                  {segment_id, SegmentType::Growing}))),
          segcore_config_(segcore_config),
          schema_(std::move(schema)),
          index_meta_(indexMeta),
          insert_record_(
              *schema_, segcore_config.get_chunk_rows(), mmap_descriptor_),
          indexing_record_(
              *schema_, index_meta_, segcore_config_, &insert_record_),
          id_(segment_id),
          deleted_record_(&insert_record_, this) {
        auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
        mcm->Register(mmap_descriptor_);

        this->CreateTextIndexes();
        this->CreateJSONIndexes();
    }

    ~SegmentGrowingImpl() {
        if (mmap_descriptor_ != nullptr) {
            auto mcm =
                storage::MmapManager::GetInstance().GetMmapChunkManager();
            mcm->UnRegister(mmap_descriptor_);
        }
    }

    void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp) const override;

    void
    vector_search(SearchInfo& search_info,
                  const void* query_data,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  SearchResult& output) const override;

    DataType
    GetFieldDataType(FieldId fieldId) const override;

 public:
    void
    mask_with_delete(BitsetTypeView& bitset,
                     int64_t ins_barrier,
                     Timestamp timestamp) const override;

    std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
    search_ids(const IdArray& id_array, Timestamp timestamp) const override;

    bool
    HasIndex(FieldId field_id) const override {
        auto& field_meta = schema_->operator[](field_id);
        if (IsVectorDataType(field_meta.get_data_type()) &&
            indexing_record_.SyncDataWithIndex(field_id)) {
            return true;
        }

        return false;
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

    std::vector<SegOffset>
    search_pk(const PkType& pk, Timestamp timestamp) const override {
        return insert_record_.search_pk(pk, timestamp);
    }

    std::vector<SegOffset>
    search_pk(const PkType& pk, int64_t insert_barrier) const override {
        return insert_record_.search_pk(pk, insert_barrier);
    }

 protected:
    int64_t
    num_chunk(FieldId field_id) const override;

    SpanBase
    chunk_data_impl(FieldId field_id, int64_t chunk_id) const override;

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    chunk_view_impl(FieldId field_id, int64_t chunk_id) const override;

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    chunk_view_by_offsets(FieldId field_id,
                          int64_t chunk_id,
                          const FixedVector<int32_t>& offsets) const override;

    std::pair<BufferView, FixedVector<bool>>
    get_chunk_buffer(FieldId field_id,
                     int64_t chunk_id,
                     int64_t start_offset,
                     int64_t length) const override {
        PanicInfo(
            ErrorCode::Unsupported,
            "get_chunk_buffer interface not supported for growing segment");
    }

    void
    check_search(const query::Plan* plan) const override {
        Assert(plan);
    }

    const ConcurrentVector<Timestamp>&
    get_timestamps() const override {
        return insert_record_.timestamps_;
    }

 private:
    void
    AddTexts(FieldId field_id,
             const std::string* texts,
             const bool* texts_valid_data,
             size_t n,
             int64_t offset_begin);

    void
    CreateTextIndexes();

    void
    AddJSONDatas(FieldId field_id,
                 const std::string* jsondatas,
                 const bool* jsondatas_valid_data,
                 size_t n,
                 int64_t offset_begin);

    void
    CreateJSONIndexes();

    void
    CreateJSONIndex(FieldId field_id);

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

const static IndexMetaPtr empty_index_meta =
    std::make_shared<CollectionIndexMeta>(1024,
                                          std::map<FieldId, FieldIndexMeta>());

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
