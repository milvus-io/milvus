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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "AckResponder.h"
#include "ConcurrentVector.h"
#include "DeletedRecord.h"
#include "InsertRecord.h"
#include "NamedType/underlying_functionalities.hpp"
#include "SegmentGrowing.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Utils.h"
#include "common/Array.h"
#include "segcore/TextLobSpillover.h"
#include "common/ArrayOffsets.h"
#include "common/BitsetView.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldMeta.h"
#include "common/GeometryCache.h"
#include "common/IndexMeta.h"
#include "common/Json.h"
#include "common/LoadInfo.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/SystemProperty.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorArray.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "geos_c.h"
#include "google/protobuf/message.h"
#include "index/contracts/query/IVectorReader.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "query/PlanImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentInterface.h"
#include "segcore/indexing/GrowingIndexSet.h"
#include "storage/MmapChunkManager.h"
#include "storage/MmapManager.h"

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
           std::shared_ptr<InsertRecordProto> insert_record_proto) override;

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
    LoadFieldData(const LoadFieldDataInfo& info,
                  milvus::OpContext* op_ctx = nullptr) override;

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
    CreateTextIndex(FieldId field_id,
                    milvus::OpContext* op_ctx = nullptr) override;

    void
    load_field_data_internal(const LoadFieldDataInfo& load_info);

    void
    load_column_group_data_internal(const LoadFieldDataInfo& load_info);

    void
    // `reserved_offset` is the logical offset PreInsert reserved for this
    // batch. Every structure this function writes -- column data, validity,
    // pk2offset_, the growing indexes, array offsets -- addresses the batch by
    // it rather than by "wherever I currently am" (#52637).
    load_field_data_common(FieldId field_id,
                           size_t reserved_offset,
                           const std::vector<FieldDataPtr>& field_data,
                           FieldId primary_field_id,
                           size_t num_rows,
                           bool text_is_remote_lob_ref = false);

    // Test-only: inject TEXT LOB base path.
    void
    SetTextLobPathForTesting(FieldId field_id, std::string lob_base_path) {
        std::unique_lock lock(text_lob_mutex_);
        text_lob_paths_[field_id] = std::move(lob_base_path);
    }

    void
    Reopen(SchemaPtr sch) override;

    void
    Reopen(
        milvus::OpContext* op_ctx,
        const milvus::proto::segcore::SegmentLoadInfo& new_load_info) override;

    void
    Reopen(milvus::OpContext* op_ctx,
           const milvus::proto::segcore::SegmentLoadInfo& new_load_info,
           SchemaPtr new_schema) override;

    void
    LazyCheckSchema(SchemaPtr sch, milvus::OpContext* op_ctx) override;

    void
    Load(milvus::tracer::TraceContext& trace_ctx,
         milvus::OpContext* op_ctx = nullptr) override;

    // Backfill fields that exist in the schema but had no data to load,
    // e.g. fields added by AddField after the loaded binlogs were written.
    // Nullable vector fields get their validity bitmap filled so queries
    // observe all-null values instead of an uninitialized column.
    void
    FillAbsentFields();

 private:
    // Build geometry cache for inserted data. reserved_offset is the batch's
    // reserved absolute segment offset: cache rows are written at absolute
    // offsets so a retried batch overwrites its own slots instead of
    // re-appending (see SimpleGeometryCache::AppendDataAt).
    void
    BuildGeometryCacheForInsert(FieldId field_id,
                                const DataArray* data_array,
                                int64_t reserved_offset,
                                int64_t num_rows);

    // Build geometry cache for loaded field data; reserved_offset as above.
    void
    BuildGeometryCacheForLoad(FieldId field_id,
                              const std::vector<FieldDataPtr>& field_data,
                              int64_t reserved_offset);

 public:
    const InsertRecord<false>&
    get_insert_record() const {
        return insert_record_;
    }

    Timestamp
    get_max_timestamp() const override {
        return insert_record_.timestamp_index_.get_max_timestamp();
    }

    SchemaPtr
    get_schema_snapshot() const override {
        return std::atomic_load_explicit(&schema_, std::memory_order_acquire);
    }

    FieldId
    get_primary_key_field_id() const {
        return primary_key_field_id_;
    }

    DataType
    get_primary_key_data_type() const {
        return primary_key_data_type_;
    }

    // count of chunk that has raw data
    int64_t
    num_chunk_data(FieldId field_id) const final {
        auto size = get_insert_record().ack_responder_.GetAck();
        return upper_div(size, segcore_config_.get_chunk_rows());
    }

    int64_t
    size_per_chunk() const final {
        return segcore_config_.get_chunk_rows();
    }

    int64_t
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
    try_remove_chunks(FieldId fieldId,
                      const Schema& schema,
                      int64_t covered_row_end);

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
                        T* output,
                        bool small_int_raw_type = false) const;

    template <typename S>
    void
    bulk_subscript_ptr_impl(
        milvus::OpContext* op_ctx,
        const VectorBase* vec_raw,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<std::string>* dst) const;

    template <typename S, typename T = S>
    void
    bulk_subscript_ptr_impl(const VectorBase* vec_raw,
                            const int64_t* seg_offsets,
                            int64_t count,
                            T* dst) const;

    template <typename SetOutput>
    void
    bulk_subscript_text_impl(FieldId field_id,
                             const VectorBase* vec_ptr,
                             const int64_t* seg_offsets,
                             int64_t count,
                             SetOutput set_output) const;

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
        const bool* valid_data,
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

    void
    bulk_subscript(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   DataType data_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* data,
                   TargetBitmap& valid_map,
                   bool small_int_raw_type = false) const override;

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
                    const std::function<void(milvus::Json, size_t, bool)>& fn,
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
          primary_key_field_id_(schema_->get_primary_field_id().value_or(
              FieldId(INVALID_FIELD_ID))),
          primary_key_data_type_(
              primary_key_field_id_.get() == INVALID_FIELD_ID
                  ? DataType::NONE
                  : schema_->operator[](primary_key_field_id_).get_data_type()),
          index_meta_(indexMeta),
          insert_record_(
              *schema_, segcore_config.get_chunk_rows(), mmap_descriptor_),
          id_(segment_id),
          deleted_record_(
              &insert_record_,
              [this](const std::vector<PkType>& pks,
                     const Timestamp* timestamps,
                     const std::function<void(const SegOffset offset,
                                              const Timestamp ts)>& callback) {
                  this->search_batch_pks(pks, timestamps, false, callback);
              },
              segment_id) {
        growing_indexes_.Initialize(
            *schema_, index_meta_, segcore_config_, id_, insert_record_);
        this->InitializeTextLobSpillovers();
        this->InitializeArrayOffsets();
        this->UpdateResourceTracking();
    }

    ~SegmentGrowingImpl() {
        // Clean up geometry cache for all fields in this segment
        auto& cache_manager =
            milvus::exec::SimpleGeometryCacheManager::Instance();
        cache_manager.RemoveSegmentCaches(segment_instance_uid(),
                                          get_segment_id());

        // Original mmap cleanup logic
        if (mmap_descriptor_ != nullptr) {
            auto mcm =
                storage::MmapManager::GetInstance().GetMmapChunkManager();
            mcm->UnRegister(mmap_descriptor_);
        }

        // Refund any tracked resources before destruction
        // No lock needed - destructor implies exclusive access
        if (tracked_resource_.AnyGTZero()) {
            Manager::GetInstance().RefundLoadedResource(
                tracked_resource_,
                fmt::format("growing_segment_{}_destructor", id_));
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
    HasIndex(FieldId field_id) const override {
        auto schema = get_schema_snapshot();
        if (!schema->has_field(field_id)) {
            return false;
        }
        return growing_indexes_.Has(field_id);
    }

    FieldIndexCapability
    IndexCapability(FieldId field_id) const override {
        return growing_indexes_.Capability(field_id);
    }

    IndexPin
    PinIndex(milvus::OpContext*, const IndexKey&) const override {
        return {};
    }

    index::GrowingIndexSnapshotPin
    PinGrowingIndex(FieldId field_id) const override {
        return growing_indexes_.PinSnapshot(field_id);
    }

    bool
    HasFieldData(FieldId field_id) const override {
        if (SystemProperty::Instance().IsSystem(field_id)) {
            return insert_record_.row_count() > 0;
        }
        if (!insert_record_.is_data_exist(field_id)) {
            return false;
        }
        if (!insert_record_.get_data_base(field_id)->empty()) {
            return true;
        }
        return IsVectorDataType(
                   get_schema_snapshot()->operator[](field_id).get_data_type()) &&
               HasRawData(field_id.get());
    }

    bool
    HasRawData(int64_t field_id) const override {
        const auto id = FieldId(field_id);
        const auto* raw = insert_record_.get_data_base(id);
        if (raw != nullptr && !raw->acquire_chunks().empty()) {
            return true;
        }
        if (!growing_indexes_.Has(id)) {
            return true;
        }
        auto pin = growing_indexes_.PinSnapshot(id);
        if (!pin) {
            // Below threshold an all-null compact column has no chunks and no
            // engine, but validity alone is a complete materialization.
            return insert_record_.is_valid_data_exist(id) &&
                   !insert_record_.get_valid_data(id)->empty();
        }
        const auto* reader =
            dynamic_cast<const index::IVectorReader*>(&pin.Reader());
        return reader != nullptr && reader->HasRawData();
    }

    bool
    CanReadRawVectorFromIndex(FieldId field_id) const {
        auto pin = growing_indexes_.PinSnapshot(field_id);
        if (!pin) {
            return false;
        }
        const auto* reader =
            dynamic_cast<const index::IVectorReader*>(&pin.Reader());
        return reader != nullptr && reader->HasRawData();
    }

    bool
    HasFieldIndexMeta(FieldId field_id) const {
        return index_meta_ != nullptr && index_meta_->HasField(field_id);
    }

    std::map<std::string, std::string>
    GetFieldIndexParams(FieldId field_id) const {
        AssertInfo(HasFieldIndexMeta(field_id),
                   "field {} has no growing index metadata",
                   field_id.get());
        return index_meta_->GetFieldIndexMeta(field_id).GetIndexParams();
    }

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n(int64_t limit, const BitsetTypeView& bitset) const override {
        return insert_record_.pk2offset_->find_first_n(limit, bitset);
    }

    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element(
        int64_t limit,
        const BitsetTypeView& element_bitset,
        const IArrayOffsets* array_offsets,
        const std::optional<QueryIteratorCursor>& cursor) const override {
        return insert_record_.pk2offset_->find_first_n_element(
            limit, element_bitset, array_offsets, cursor);
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
        auto schema = get_schema_snapshot();
        return schema->get_fields().find(field_id) !=
               schema->get_fields().end();
    }

    /**
     * @brief Check if a TEXT field has spillover enabled
     */
    bool
    HasTextLobSpillover(FieldId field_id) const {
        std::shared_lock lock(text_lob_mutex_);
        return text_lob_spillovers_.find(field_id) !=
               text_lob_spillovers_.end();
    }

    /**
     * @brief Get TextLobSpillover for a TEXT field (for query/flush paths)
     * @return Pointer to TextLobSpillover, or nullptr if not found
     */
    TextLobSpillover*
    GetTextLobSpillover(FieldId field_id) const {
        std::shared_lock lock(text_lob_mutex_);
        auto it = text_lob_spillovers_.find(field_id);
        if (it != text_lob_spillovers_.end()) {
            return it->second.get();
        }
        return nullptr;
    }

    std::shared_ptr<const IArrayOffsets>
    GetArrayOffsets(FieldId field_id) const override {
        std::shared_lock lock(array_offsets_map_mutex_);
        auto it = array_offsets_map_.find(field_id);
        if (it != array_offsets_map_.end()) {
            return it->second;
        }
        return nullptr;
    }
    struct ValidResult {
        int64_t valid_count = 0;
        std::unique_ptr<bool[]> valid_data;
        // NULL filtering preserves logical segment offsets and their order.
        std::vector<int64_t> valid_logical_offsets;
    };

    ValidResult
    FilterVectorValidOffsets(milvus::OpContext* op_ctx,
                             FieldId field_id,
                             const int64_t* seg_offsets,
                             int64_t count) const;

    /**
     * @brief Estimate the current total resource usage of the growing segment
     *
     * This includes memory/disk usage for:
     * - Field data (raw vectors and scalars)
     * - Timestamps
     * - PK-to-offset index
     * - Interim vector indexes (if enabled)
     * - Text match indexes (if enabled)
     * - Deleted records
     *
     * @return ResourceUsage containing memory_bytes and file_bytes estimates
     */
    ResourceUsage
    EstimateSegmentResourceUsage() const;

    ResourceUsage
    EstimateSegmentResourceUsage(const Schema& schema) const;

    void
    ApplyFieldValidData(milvus::OpContext* op_ctx,
                        FieldId field_id,
                        int64_t chunk_id,
                        int64_t offset,
                        int64_t size,
                        TargetBitmapView valid_result) const override;

    void
    ApplyFieldValidDataByOffsets(milvus::OpContext* op_ctx,
                                 FieldId field_id,
                                 const int64_t* offsets,
                                 int64_t count,
                                 TargetBitmapView valid_result) const override;

 protected:
    int64_t
    num_chunk(FieldId field_id) const override;

    PinWrapper<SpanBase>
    chunk_data_impl(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    int64_t chunk_id) const override;

    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    chunk_string_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<ArrayView>, ValidityView>>
    chunk_array_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<ArrayValueView>, ValidityView>>
    chunk_array_value_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<VectorArrayView>, ValidityView>>
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

    PinWrapper<std::pair<std::vector<ArrayValueView>, FixedVector<bool>>>
    chunk_array_value_views_by_offsets(
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
    fill_empty_field(const FieldMeta& field_meta, int64_t total_row_num);

    void
    EnsureArrayOffsetsForStructField(const FieldMeta& field_meta,
                                     int64_t row_count);

    void
    EnsureArrayOffsetsForStructField(const FieldMeta& field_meta,
                                     int64_t row_count,
                                     const Schema& schema);

    /**
     * @brief Update resource tracking by refunding old estimate and charging new
     *
     * This method:
     * 1. Estimates current total resource usage of the growing segment
     * 2. Refunds the previously tracked resource from the cache manager
     * 3. Charges the new resource usage to the cache manager
     * 4. Updates the tracked resource checkpoint
     *
     * Should be called after data modifications (Insert, Delete, etc.)
     */
    void
    UpdateResourceTracking();

    void
    UpdateResourceTracking(const Schema& schema);

 private:
    void
    CompleteGrowingRawRange(int64_t row_begin,
                            int64_t row_end,
                            bool require_index_flush);

    void
    PublishGrowingIndexesThroughRawReady();

    void
    FeedGrowingIndexRange(const FieldMeta& field_meta,
                          int64_t row_begin,
                          int64_t row_end,
                          GrowingIndexSet::Appender* staged = nullptr);

    void
    StageInsertVectorInput(
        int64_t row_begin,
        int64_t row_end,
        const std::shared_ptr<InsertRecordProto>& insert_record,
        const std::unordered_map<FieldId, int64_t>& field_offsets);

    void
    StageLoadedVectorInput(FieldId field_id,
                           int64_t row_begin,
                           int64_t row_end,
                           const std::vector<FieldDataPtr>& field_data);

    void
    FillAbsentFieldsThrough(int64_t row_end);

    void
    EnsureTextLobSpillover(FieldId field_id);

    /**
     * @brief Initialize TEXT LOB spillover files for each TEXT field
     *
     * Creates TextLobSpillover instances for all TEXT fields in the schema.
     * TEXT data will be written to temporary LOB files to reduce memory usage.
     */
    void
    InitializeTextLobSpillovers();

    /**
     * @brief Initialize TEXT LOB paths from manifest path (for reload from V3 storage)
     *
     * Similar to ChunkedSegmentSealedImpl::InitTextLobPaths.
     * Resolves LOBReferences at query time via TextColumnCache.
     */
    void
    InitTextLobPaths(const std::string& manifest_path);

    /**
     * @brief Check if a TEXT field has LOB path (reload from V3 storage)
     */
    bool
    HasTextLobPath(FieldId field_id) const {
        std::shared_lock lock(text_lob_mutex_);
        return text_lob_paths_.find(field_id) != text_lob_paths_.end();
    }

    /**
     * @brief Load all column groups from a manifest file path
     *
     * This method parses the manifest path to retrieve column groups metadata
     * and loads each column group into the growing segment.
     *
     * @param manifest_path JSON string containing base_path and version fields
     */
    void
    LoadColumnsGroups(std::string manifest_path);

    /**
     * @brief Load a single column group and return field data
     *
     * Reads a specific column group from milvus storage and converts it to
     * field data format that can be inserted into the growing segment.
     *
     * @param column_groups Metadata about all available column groups
     * @param properties Storage properties for accessing the data
     * @param index Index of the column group to load
     * @return Map of field IDs to their corresponding field data vectors
     */
    std::unordered_map<FieldId, std::vector<FieldDataPtr>>
    LoadColumnGroup(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        int64_t index,
        int64_t row_limit);

    void
    InitializeArrayOffsets();

 private:
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    SegcoreConfig segcore_config_;
    SchemaPtr schema_;
    FieldId primary_key_field_id_{INVALID_FIELD_ID};
    DataType primary_key_data_type_{DataType::NONE};
    IndexMetaPtr index_meta_;

    // inserted fields data and row_ids, timestamps
    InsertRecord<false> insert_record_;

    // No chunk lock. Readers pin the generation they walk via
    // VectorBase::acquire_chunks(); try_remove_chunks swaps the container's
    // collection out and lets the last pin holder free it. Reclamation and
    // reads no longer exclude each other in either direction.

    GrowingIndexSet growing_indexes_;
    AckResponder growing_raw_ready_;
    mutable std::mutex growing_index_feed_mutex_;
    int64_t growing_index_feed_cursor_{0};
    std::optional<int64_t> growing_index_pending_feed_end_;
    // A load range must not become query-visible until every owner has
    // synchronously published through this row end. Keep the boundary across
    // failures so a later Insert retries the flush before publishing rows.
    std::optional<int64_t> growing_index_required_flush_end_;

    struct PendingVectorInput {
        struct FieldLocation {
            // Number of valid physical rows before each fixed-size logical
            // block. This bounds nullable input location work per feed window
            // without retaining a per-row map.
            std::vector<int64_t> valid_prefix;
            std::vector<FieldDataPtr> loaded_fields;
            // Loaded inputs may consist of multiple FieldData objects. These
            // cumulative ends locate both the logical part and its compact
            // physical payload without rescanning earlier objects.
            std::vector<int64_t> loaded_row_ends;
            std::vector<int64_t> loaded_physical_ends;
        };

        int64_t row_end{0};
        std::shared_ptr<InsertRecordProto> insert_record;
        std::unordered_map<FieldId, int64_t> insert_field_offsets;
        std::unordered_map<FieldId, FieldLocation> field_locations;
    };
    // Keyed by Segment row begin. Entries retain the original typed payload
    // until all owners accept and the main visibility ACK advances.
    std::map<int64_t, PendingVectorInput> pending_vector_inputs_;

    // deleted pks
    mutable DeletedRecord<false> deleted_record_;

    int64_t id_;

    SegmentStats stats_{};

    // milvus storage internal api reader instance
    std::unique_ptr<milvus_storage::api::Reader> reader_;

    // field_id -> ArrayOffsetsGrowing (for fast lookup via GetArrayOffsets)
    // Multiple field_ids from the same struct point to the same ArrayOffsetsGrowing
    std::unordered_map<FieldId, std::shared_ptr<ArrayOffsetsGrowing>>
        array_offsets_map_;

    // Representative field_id for each struct (used to extract array lengths during Insert)
    // One field_id per struct, since all fields in the same struct have identical array lengths
    std::unordered_set<FieldId> struct_representative_fields_;

    mutable std::shared_mutex array_offsets_map_mutex_;

    // Tracked resource usage for refund-then-charge pattern
    // This stores the last estimated resource usage that was charged to the cache manager
    ResourceUsage tracked_resource_{};
    // Mutex to protect tracked_resource_ updates (refund-then-charge must be atomic)
    mutable std::mutex resource_tracking_mutex_;

    // TEXT field spillover: field_id -> TextLobSpillover
    // TEXT data is written to temporary LOB files to reduce memory usage.
    // Memory stores only 16-byte references (offset, size, flags).
    std::unordered_map<FieldId, std::unique_ptr<TextLobSpillover>>
        text_lob_spillovers_;

    // TEXT field LOB paths for V3 storage reload (same as sealed segment)
    // field_id -> LOB base path on remote storage
    // LOBReferences in ConcurrentVector are resolved at query time via TextColumnCache
    std::unordered_map<FieldId, std::string> text_lob_paths_;

    // Per-field boundary between remote V3 LOB references and locally written
    // spillover references. A TEXT field introduced by Reopen has no remote
    // prefix even when older TEXT fields do.
    std::unordered_map<FieldId, int64_t> text_loaded_row_counts_;
    mutable std::shared_mutex text_lob_mutex_;
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
