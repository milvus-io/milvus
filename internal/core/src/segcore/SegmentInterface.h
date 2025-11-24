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

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <index/ScalarIndex.h>

#include "cachinglayer/CacheSlot.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/SystemProperty.h"
#include "common/Types.h"
#include "common/LoadInfo.h"
#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/QueryInfo.h"
#include "folly/SharedMutex.h"
#include "common/type_c.h"
#include "mmap/ChunkedColumnInterface.h"
#include "index/Index.h"
#include "index/JsonFlatIndex.h"
#include "query/Plan.h"
#include "pb/segcore.pb.h"
#include "index/SkipIndex.h"
#include "index/TextMatchIndex.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/InsertRecord.h"
#include "index/NgramInvertedIndex.h"
#include "index/json_stats/JsonKeyStats.h"

namespace milvus::segcore {

using namespace milvus::cachinglayer;

struct SegmentStats {
    // we stat the memory size used by the segment,
    // including the insert data and delete data.
    std::atomic<size_t> mem_size{};
};

// common interface of SegmentSealed and SegmentGrowing used by C API
class SegmentInterface {
 public:
    virtual ~SegmentInterface() = default;

    virtual void
    FillPrimaryKeys(const query::Plan* plan, SearchResult& results) const = 0;

    virtual void
    FillTargetEntry(const query::Plan* plan, SearchResult& results) const = 0;

    virtual bool
    Contain(const PkType& pk) const = 0;

    virtual std::unique_ptr<SearchResult>
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_group,
           Timestamp timestamp,
           int32_t consistency_level = 0,
           Timestamp collection_ttl = 0) const = 0;

    virtual std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(tracer::TraceContext* trace_ctx,
             const query::RetrievePlan* Plan,
             Timestamp timestamp,
             int64_t limit_size,
             bool ignore_non_pk,
             int32_t consistency_level = 0,
             Timestamp collection_ttl = 0) const = 0;

    virtual std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(tracer::TraceContext* trace_ctx,
             const query::RetrievePlan* Plan,
             const int64_t* offsets,
             int64_t size) const = 0;

    virtual size_t
    GetMemoryUsageInBytes() const = 0;

    virtual int64_t
    get_row_count() const = 0;

    virtual const Schema&
    get_schema() const = 0;

    virtual int64_t
    get_deleted_count() const = 0;

    virtual int64_t
    get_real_count() const = 0;

    virtual int64_t
    get_field_avg_size(FieldId field_id) const = 0;

    virtual void
    set_field_avg_size(FieldId field_id,
                       int64_t num_rows,
                       int64_t field_size) = 0;

    virtual SegcoreError
    Delete(int64_t size, const IdArray* pks, const Timestamp* timestamps) = 0;

    virtual void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) = 0;

    virtual void
    LoadFieldData(const LoadFieldDataInfo& info) = 0;

    virtual int64_t
    get_segment_id() const = 0;

    virtual SegmentType
    type() const = 0;

    virtual bool
    HasRawData(int64_t field_id) const = 0;

    virtual bool
    HasFieldData(FieldId field_id) const = 0;

    virtual bool
    is_nullable(FieldId field_id) const = 0;

    virtual void
    CreateTextIndex(FieldId field_id) = 0;

    virtual PinWrapper<index::TextMatchIndex*>
    GetTextIndex(milvus::OpContext* op_ctx, FieldId field_id) const = 0;

    virtual std::vector<PinWrapper<const index::IndexBase*>>
    PinJsonIndex(milvus::OpContext* op_ctx,
                 FieldId field_id,
                 const std::string& path,
                 DataType data_type,
                 bool any_type,
                 bool is_array) const {
        return {};
    }

    virtual std::vector<PinWrapper<const index::IndexBase*>>
    PinIndex(milvus::OpContext* op_ctx,
             FieldId field_id,
             bool include_ngram = false) const {
        return {};
    };

    virtual void
    BulkGetJsonData(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    std::function<void(milvus::Json, size_t, bool)> fn,
                    const int64_t* offsets,
                    int64_t count) const = 0;

    virtual PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx, FieldId field_id) const = 0;

    virtual PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         const std::string& nested_path) const = 0;

    virtual PinWrapper<index::JsonKeyStats*>
    GetJsonStats(milvus::OpContext* op_ctx, FieldId field_id) const = 0;

    virtual void
    LoadJsonStats(FieldId field_id, index::CacheJsonKeyStatsPtr cache_slot) = 0;

    virtual void
    RemoveJsonStats(FieldId field_id) = 0;

    virtual void
    LazyCheckSchema(SchemaPtr sch) = 0;

    // reopen segment with new schema
    virtual void
    Reopen(SchemaPtr sch) = 0;

    // FinishLoad notifies the segment that all load operation are done
    // currently it's used to sync field data list with updated schema.
    virtual void
    FinishLoad() = 0;

    virtual void
    SetLoadInfo(const milvus::proto::segcore::SegmentLoadInfo& load_info) = 0;

    virtual void
    Load(milvus::tracer::TraceContext& trace_ctx) = 0;
};

// internal API for DSL calculation
// only for implementation
class SegmentInternalInterface : public SegmentInterface {
 public:
    virtual void
    prefetch_chunks(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    const std::vector<int64_t>& chunk_ids) const {
        // do nothing
    }

    template <typename T>
    PinWrapper<Span<T>>
    chunk_data(milvus::OpContext* op_ctx,
               FieldId field_id,
               int64_t chunk_id) const {
        return chunk_data_impl(op_ctx, field_id, chunk_id)
            .transform<Span<T>>([](SpanBase&& span_base) {
                return static_cast<Span<T>>(span_base);
            });
    }

    template <typename ViewType>
    PinWrapper<std::pair<std::vector<ViewType>, FixedVector<bool>>>
    chunk_view(milvus::OpContext* op_ctx,
               FieldId field_id,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const {
        if constexpr (std::is_same_v<ViewType, std::string_view>) {
            return chunk_string_view_impl(
                op_ctx, field_id, chunk_id, offset_len);
        } else if constexpr (std::is_same_v<ViewType, ArrayView>) {
            return chunk_array_view_impl(
                op_ctx, field_id, chunk_id, offset_len);
        } else if constexpr (std::is_same_v<ViewType, VectorArrayView>) {
            return chunk_vector_array_view_impl(
                op_ctx, field_id, chunk_id, offset_len);
        } else if constexpr (std::is_same_v<ViewType, Json>) {
            auto pw =
                chunk_string_view_impl(op_ctx, field_id, chunk_id, offset_len);
            auto [string_views, valid_data] = pw.get();
            std::vector<Json> res;
            res.reserve(string_views.size());
            for (const auto& str_view : string_views) {
                res.emplace_back(Json(str_view));
            }
            return PinWrapper<
                std::pair<std::vector<ViewType>, FixedVector<bool>>>(
                pw, {std::move(res), std::move(valid_data)});
        }
    }

    template <typename ViewType>
    PinWrapper<std::pair<std::vector<ViewType>, FixedVector<bool>>>
    get_batch_views(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    int64_t chunk_id,
                    int64_t start_offset,
                    int64_t length) const {
        if (this->type() == SegmentType::Growing) {
            ThrowInfo(ErrorCode::Unsupported,
                      "get chunk views not supported for growing segment");
        }
        return chunk_view<ViewType>(
            op_ctx, field_id, chunk_id, std::make_pair(start_offset, length));
    }

    template <typename ViewType>
    PinWrapper<std::pair<std::vector<ViewType>, FixedVector<bool>>>
    get_views_by_offsets(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const {
        if (this->type() == SegmentType::Growing) {
            ThrowInfo(ErrorCode::Unsupported,
                      "get chunk views not supported for growing segment");
        }
        if constexpr (std::is_same_v<ViewType, std::string_view>) {
            return chunk_string_views_by_offsets(
                op_ctx, field_id, chunk_id, offsets);
        } else if constexpr (std::is_same_v<ViewType, Json>) {
            auto pw = chunk_string_views_by_offsets(
                op_ctx, field_id, chunk_id, offsets);
            std::vector<ViewType> res;
            res.reserve(pw.get().first.size());
            for (const auto& view : pw.get().first) {
                res.emplace_back(view);
            }
            return PinWrapper<
                std::pair<std::vector<ViewType>, FixedVector<bool>>>(
                {std::move(res), pw.get().second});
        } else if constexpr (std::is_same_v<ViewType, ArrayView>) {
            return chunk_array_views_by_offsets(
                op_ctx, field_id, chunk_id, offsets);
        }
    }

    // union(segment_id, field_id) as unique id
    virtual std::string
    GetUniqueFieldId(int64_t field_id) const {
        return std::to_string(get_segment_id()) + "_" +
               std::to_string(field_id);
    }

    std::unique_ptr<SearchResult>
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_group,
           Timestamp timestamp,
           int32_t consistency_level = 0,
           Timestamp collection_ttl = 0) const override;

    void
    FillPrimaryKeys(const query::Plan* plan,
                    SearchResult& results) const override;

    void
    FillTargetEntry(const query::Plan* plan,
                    SearchResult& results) const override;

    std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(tracer::TraceContext* trace_ctx,
             const query::RetrievePlan* Plan,
             Timestamp timestamp,
             int64_t limit_size,
             bool ignore_non_pk,
             int32_t consistency_level = 0,
             Timestamp collection_ttl = 0) const override;

    std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(tracer::TraceContext* trace_ctx,
             const query::RetrievePlan* Plan,
             const int64_t* offsets,
             int64_t size) const override;

    virtual bool
    HasIndex(FieldId field_id) const = 0;

    int64_t
    get_real_count() const override;

    int64_t
    get_field_avg_size(FieldId field_id) const override;

    void
    set_field_avg_size(FieldId field_id,
                       int64_t num_rows,
                       int64_t field_size) override;
    virtual bool
    is_chunked() const {
        return false;
    }

    const SkipIndex&
    GetSkipIndex() const;

    void
    LoadSkipIndex(FieldId field_id,
                  DataType data_type,
                  std::shared_ptr<ChunkedColumnInterface> column) {
        skip_index_.LoadSkip(get_segment_id(), field_id, data_type, column);
    }

    void
    LoadSkipIndexFromStatistics(
        FieldId field_id,
        DataType data_type,
        std::vector<std::shared_ptr<parquet::Statistics>> statistics) {
        skip_index_.LoadSkipFromStatistics(
            get_segment_id(), field_id, data_type, statistics);
    }

    virtual DataType
    GetFieldDataType(FieldId fieldId) const = 0;

    PinWrapper<index::TextMatchIndex*>
    GetTextIndex(milvus::OpContext* op_ctx, FieldId field_id) const override;

    virtual PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx, FieldId field_id) const override;

    virtual PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         const std::string& nested_path) const override;

    virtual void
    SetLoadInfo(
        const milvus::proto::segcore::SegmentLoadInfo& load_info) override {
        load_info_ = load_info;
    }

 public:
    // `query_offsets` is not null only for vector array (embedding list) search
    // where it denotes the number of vectors in each embedding list. The length
    // of `query_offsets` is the number of queries in the search plus one (the first
    // element in query_offsets is 0).
    virtual void
    vector_search(SearchInfo& search_info,
                  const void* query_data,
                  const size_t* query_offsets,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  milvus::OpContext* op_context,
                  SearchResult& output) const = 0;

    virtual void
    mask_with_delete(BitsetTypeView& bitset,
                     int64_t ins_barrier,
                     Timestamp timestamp) const = 0;

    // count of chunk that has raw data
    virtual int64_t
    num_chunk_data(FieldId field_id) const = 0;

    virtual int64_t
    num_rows_until_chunk(FieldId field_id, int64_t chunk_id) const = 0;

    // bitset 1 means not hit. 0 means hit.
    virtual void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp,
                         Timestamp collection_ttl) const = 0;

    // count of chunks
    virtual int64_t
    num_chunk(FieldId field_id) const = 0;

    virtual int64_t
    chunk_size(FieldId field_id, int64_t chunk_id) const = 0;

    virtual std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field_id, int64_t offset) const = 0;

    // element size in each chunk
    virtual int64_t
    size_per_chunk() const = 0;

    virtual int64_t
    get_active_count(Timestamp ts) const = 0;

    /**
     * search offset by possible pk values and mvcc timestamp
     *
     * @param bitset The final bitset after id array filtering,
     *  `false` means that the entity will be filtered out.
     * @param id_array possible pk values
     * this interface is used for internal expression calculation,
     * so no need timestamp parameter, mvcc node prove the timestamp is already filtered.
     */
    virtual void
    search_ids(BitsetType& bitset, const IdArray& id_array) const = 0;

    /**
     * Apply timestamp filtering on bitset, the query can't see an entity whose
     * timestamp is bigger than the timestamp of query.
     *
     * @param bitset The final bitset after scalar filtering and delta filtering,
     *  `false` means that the entity will be filtered out.
     * @param timestamp The timestamp of query.
     */
    void
    timestamp_filter(BitsetType& bitset, Timestamp timestamp) const;

    /**
     * Apply timestamp filtering on bitset, the query can't see an entity whose
     * timestamp is bigger than the timestamp of query. The passed offsets are
     * all candidate entities.
     *
     * @param bitset The final bitset after scalar filtering and delta filtering,
     *  `true` means that the entity will be filtered out.
     * @param offsets The segment offsets of all candidates.
     * @param timestamp The timestamp of query.
     */
    void
    timestamp_filter(BitsetType& bitset,
                     const std::vector<int64_t>& offsets,
                     Timestamp timestamp) const;

    /**
     * Sort all candidates in ascending order, and then return the limit smallest.
     * Bitset is used to check if the candidate will be filtered out. `false_filtered_out`
     * determines how to filter out candidates. If `false_filtered_out` is true, we will
     * filter all candidates whose related bit is false.
     *
     * @param limit
     * @param bitset
     * @param false_filtered_out
     * @return All candidates offsets.
     */
    virtual std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first(int64_t limit, const BitsetType& bitset) const = 0;

    void
    FillTargetEntry(
        tracer::TraceContext* trace_ctx,
        const query::RetrievePlan* plan,
        const std::unique_ptr<proto::segcore::RetrieveResults>& results,
        const int64_t* offsets,
        int64_t size,
        bool ignore_non_pk,
        bool fill_ids) const;

    // return whether field mmap or not
    virtual bool
    is_mmap_field(FieldId field_id) const = 0;

    virtual std::unique_ptr<DataArray>
    bulk_subscript_not_exist_field(const milvus::FieldMeta& field_meta,
                                   int64_t count) const;

 protected:
    // todo: use an Unified struct for all type in growing/seal segment to store data and valid_data.
    // internal API: return chunk_data in span
    virtual PinWrapper<SpanBase>
    chunk_data_impl(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    int64_t chunk_id) const = 0;

    // internal API: return chunk string views in vector
    virtual PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    chunk_string_view_impl(milvus::OpContext* op_ctx,
                           FieldId field_id,
                           int64_t chunk_id,
                           std::optional<std::pair<int64_t, int64_t>>
                               offset_len = std::nullopt) const = 0;

    virtual PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    chunk_array_view_impl(milvus::OpContext* op_ctx,
                          FieldId field_id,
                          int64_t chunk_id,
                          std::optional<std::pair<int64_t, int64_t>>
                              offset_len = std::nullopt) const = 0;

    virtual PinWrapper<
        std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    chunk_vector_array_view_impl(milvus::OpContext* op_ctx,
                                 FieldId field_id,
                                 int64_t chunk_id,
                                 std::optional<std::pair<int64_t, int64_t>>
                                     offset_len = std::nullopt) const = 0;

    virtual PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    chunk_string_views_by_offsets(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const = 0;

    virtual PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    chunk_array_views_by_offsets(milvus::OpContext* op_ctx,
                                 FieldId field_id,
                                 int64_t chunk_id,
                                 const FixedVector<int32_t>& offsets) const = 0;

    virtual void
    check_search(const query::Plan* plan) const = 0;

    virtual const ConcurrentVector<Timestamp>&
    get_timestamps() const = 0;

 public:
    virtual bool
    is_field_exist(FieldId field_id) const = 0;
    // calculate output[i] = Vec[seg_offsets[i]}, where Vec binds to system_type
    virtual void
    bulk_subscript(milvus::OpContext* op_ctx,
                   SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const = 0;

    // calculate output[i] = Vec[seg_offsets[i]}, where Vec binds to field_offset
    virtual std::unique_ptr<DataArray>
    bulk_subscript(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   const int64_t* seg_offsets,
                   int64_t count) const = 0;

    virtual std::unique_ptr<DataArray>
    bulk_subscript(
        milvus::OpContext* op_ctx,
        FieldId field_ids,
        const int64_t* seg_offsets,
        int64_t count,
        const std::vector<std::string>& dynamic_field_names) const = 0;

    virtual void
    pk_range(milvus::OpContext* op_ctx,
             proto::plan::OpType op,
             const PkType& pk,
             BitsetTypeView& bitset) const = 0;

    virtual GEOSContextHandle_t
    get_ctx() const {
        return ctx_;
    };

 protected:
    // mutex protecting rw options on schema_
    std::shared_mutex sch_mutex_;

    milvus::proto::segcore::SegmentLoadInfo load_info_;

    mutable std::shared_mutex mutex_;
    // fieldID -> std::pair<num_rows, avg_size>
    std::unordered_map<FieldId, std::pair<int64_t, int64_t>>
        variable_fields_avg_size_;  // bytes;
    SkipIndex skip_index_;

    // text-indexes used to do match.
    std::unordered_map<
        FieldId,
        std::variant<std::unique_ptr<milvus::index::TextMatchIndex>,
                     std::shared_ptr<milvus::index::TextMatchIndexHolder>,
                     std::shared_ptr<milvus::cachinglayer::CacheSlot<
                         milvus::index::TextMatchIndex>>>>
        text_indexes_;

    // json stats cache (field_id -> CacheSlot of JsonKeyStats)
    mutable folly::Synchronized<
        std::unordered_map<FieldId, index::CacheJsonKeyStatsPtr>>
        json_stats_;

    GEOSContextHandle_t ctx_ = GEOS_init_r();
};

}  // namespace milvus::segcore
