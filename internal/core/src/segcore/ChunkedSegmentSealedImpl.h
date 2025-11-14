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
#include "parquet/statistics.h"
#include "segcore/IndexConfigGenerator.h"
#include "segcore/SegcoreConfig.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#include "index/json_stats/JsonKeyStats.h"
#include "pb/index_cgo_msg.pb.h"

namespace milvus::segcore {

namespace storagev1translator {
class InsertRecordTranslator;
}

using namespace milvus::cachinglayer;

class ChunkedSegmentSealedImpl : public SegmentSealed {
 public:
    using ParquetStatistics = std::vector<std::shared_ptr<parquet::Statistics>>;
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

    void
    SetLoadInfo(
        const milvus::proto::segcore::SegmentLoadInfo& load_info) override;

    void
    Load(milvus::tracer::TraceContext& trace_ctx) override;

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
    load_system_field_internal(FieldId field_id, FieldDataInfo& data);

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
        bool is_proxy_column,
        std::optional<ParquetStatistics> statistics = {});

    // Convert proto::segcore::FieldIndexInfo to LoadIndexInfo
    LoadIndexInfo
    ConvertFieldIndexInfoToLoadIndexInfo(
        const milvus::proto::segcore::FieldIndexInfo* field_index_info) const;

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
    milvus::proto::segcore::SegmentLoadInfo segment_load_info_;

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
