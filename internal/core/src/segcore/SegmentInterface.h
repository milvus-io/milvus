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
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <index/ScalarIndex.h>

#include "FieldIndexing.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/SystemProperty.h"
#include "common/Types.h"
#include "common/LoadInfo.h"
#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/QueryInfo.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "index/IndexInfo.h"
#include "index/SkipIndex.h"
#include "mmap/Column.h"
#include "index/TextMatchIndex.h"
#include "index/JsonKeyStatsInvertedIndex.h"

namespace milvus::segcore {

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
           int32_t consistency_level = 0) const = 0;

    virtual std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(tracer::TraceContext* trace_ctx,
             const query::RetrievePlan* Plan,
             Timestamp timestamp,
             int64_t limit_size,
             bool ignore_non_pk,
             int32_t consistency_level = 0) const = 0;

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

    //  virtual int64_t
    //  PreDelete(int64_t size) = 0;

    virtual SegcoreError
    Delete(int64_t reserved_offset,
           int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) = 0;

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
    is_nullable(FieldId field_id) const = 0;

    virtual void
    CreateTextIndex(FieldId field_id) = 0;

    virtual index::TextMatchIndex*
    GetTextIndex(FieldId field_id) const = 0;

    virtual index::IndexBase*
    GetJsonIndex(FieldId field_id, std::string path) const {
        return nullptr;
    }
    virtual index::JsonKeyStatsInvertedIndex*
    GetJsonKeyIndex(FieldId field_id) const = 0;

    virtual std::pair<std::string_view, bool>
    GetJsonData(FieldId field_id, size_t offset) const = 0;

    virtual void
    lazy_check_schema(const query::Plan* plan) = 0;

    // reopen segment with new schema
    virtual void
    reopen(SchemaPtr sch) = 0;

    // finish_load notifies the segment that all load operation are done
    // currently it's used to sync field data list with updated schema.
    virtual void
    finish_load() = 0;
};

// internal API for DSL calculation
// only for implementation
class SegmentInternalInterface : public SegmentInterface {
 public:
    template <typename T>
    Span<T>
    chunk_data(FieldId field_id, int64_t chunk_id) const {
        return static_cast<Span<T>>(chunk_data_impl(field_id, chunk_id));
    }

    template <typename ViewType>
    std::pair<std::vector<ViewType>, FixedVector<bool>>
    chunk_view(FieldId field_id,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const {
        if constexpr (std::is_same_v<ViewType, std::string_view>) {
            auto [string_views, valid_data] =
                chunk_string_view_impl(field_id, chunk_id, offset_len);
            return std::make_pair(std::move(string_views),
                                  std::move(valid_data));
        } else if constexpr (std::is_same_v<ViewType, ArrayView>) {
            auto [array_views, valid_data] =
                chunk_array_view_impl(field_id, chunk_id, offset_len);
            return std::make_pair(array_views, valid_data);
        } else if constexpr (std::is_same_v<ViewType, Json>) {
            auto [string_views, valid_data] =
                chunk_string_view_impl(field_id, chunk_id, offset_len);
            std::vector<Json> res;
            res.reserve(string_views.size());
            for (const auto& str_view : string_views) {
                res.emplace_back(str_view);
            }
            return {std::move(res), std::move(valid_data)};
        }
    }

    template <typename ViewType>
    std::pair<std::vector<ViewType>, FixedVector<bool>>
    get_batch_views(FieldId field_id,
                    int64_t chunk_id,
                    int64_t start_offset,
                    int64_t length) const {
        if (this->type() == SegmentType::Growing) {
            PanicInfo(ErrorCode::Unsupported,
                      "get chunk views not supported for growing segment");
        }
        return chunk_view<ViewType>(
            field_id, chunk_id, std::make_pair(start_offset, length));
    }

    template <typename ViewType>
    std::pair<std::vector<ViewType>, FixedVector<bool>>
    get_views_by_offsets(FieldId field_id,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const {
        if (this->type() == SegmentType::Growing) {
            PanicInfo(ErrorCode::Unsupported,
                      "get chunk views not supported for growing segment");
        }
        auto chunk_view = chunk_view_by_offsets(field_id, chunk_id, offsets);
        if constexpr (std::is_same_v<ViewType, std::string_view>) {
            return chunk_view;
        } else {
            std::vector<ViewType> res;
            res.reserve(chunk_view.first.size());
            for (const auto& view : chunk_view.first) {
                res.emplace_back(view);
            }
            return {res, chunk_view.second};
        }
    }

    template <typename T>
    const index::ScalarIndex<T>&
    chunk_scalar_index(FieldId field_id, int64_t chunk_id) const {
        static_assert(IsScalar<T>);
        using IndexType = index::ScalarIndex<T>;
        auto base_ptr = chunk_index_impl(field_id, chunk_id);
        auto ptr = dynamic_cast<const IndexType*>(base_ptr);
        AssertInfo(ptr, "entry mismatch");
        return *ptr;
    }

    // union(segment_id, field_id) as unique id
    virtual std::string
    GetUniqueFieldId(int64_t field_id) const {
        return std::to_string(get_segment_id()) + "_" +
               std::to_string(field_id);
    }

    template <typename T>
    const index::ScalarIndex<T>&
    chunk_scalar_index(FieldId field_id,
                       std::string path,
                       int64_t chunk_id) const {
        using IndexType = index::ScalarIndex<T>;
        auto base_ptr = chunk_index_impl(field_id, path, chunk_id);
        auto ptr = dynamic_cast<const IndexType*>(base_ptr);
        AssertInfo(ptr, "entry mismatch");
        return *ptr;
    }

    std::unique_ptr<SearchResult>
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_group,
           Timestamp timestamp,
           int32_t consistency_level = 0) const override;

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
             int32_t consistency_level = 0) const override;

    std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(tracer::TraceContext* trace_ctx,
             const query::RetrievePlan* Plan,
             const int64_t* offsets,
             int64_t size) const override;

    virtual bool
    HasIndex(FieldId field_id) const = 0;

    virtual bool
    HasIndex(FieldId field_id,
             const std::string& nested_path,
             DataType data_type,
             bool any_type = false) const = 0;

    virtual bool
    HasFieldData(FieldId field_id) const = 0;

    virtual std::string
    debug() const = 0;

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
    LoadPrimitiveSkipIndex(FieldId field_id,
                           int64_t chunk_id,
                           DataType data_type,
                           const void* chunk_data,
                           const bool* valid_data,
                           int64_t count);

    template <typename T>
    void
    LoadStringSkipIndex(FieldId field_id,
                        int64_t chunk_id,
                        const T& var_column) {
        skip_index_.LoadString(field_id, chunk_id, var_column);
    }

    virtual DataType
    GetFieldDataType(FieldId fieldId) const = 0;

    index::TextMatchIndex*
    GetTextIndex(FieldId field_id) const override;

    virtual index::JsonKeyStatsInvertedIndex*
    GetJsonKeyIndex(FieldId field_id) const override;

 public:
    virtual void
    vector_search(SearchInfo& search_info,
                  const void* query_data,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  SearchResult& output) const = 0;

    virtual void
    mask_with_delete(BitsetTypeView& bitset,
                     int64_t ins_barrier,
                     Timestamp timestamp) const = 0;

    // count of chunk that has index available
    virtual int64_t
    num_chunk_index(FieldId field_id) const = 0;

    // count of chunk that has raw data
    virtual int64_t
    num_chunk_data(FieldId field_id) const = 0;

    virtual int64_t
    num_rows_until_chunk(FieldId field_id, int64_t chunk_id) const = 0;

    // bitset 1 means not hit. 0 means hit.
    virtual void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp) const = 0;

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

    virtual std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
    search_ids(const IdArray& id_array, Timestamp timestamp) const = 0;

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
    virtual SpanBase
    chunk_data_impl(FieldId field_id, int64_t chunk_id) const = 0;

    // internal API: return chunk string views in vector
    virtual std::pair<std::vector<std::string_view>, FixedVector<bool>>
    chunk_string_view_impl(FieldId field_id,
                           int64_t chunk_id,
                           std::optional<std::pair<int64_t, int64_t>>
                               offset_len = std::nullopt) const = 0;

    virtual std::pair<std::vector<ArrayView>, FixedVector<bool>>
    chunk_array_view_impl(FieldId field_id,
                          int64_t chunk_id,
                          std::optional<std::pair<int64_t, int64_t>>
                              offset_len = std::nullopt) const = 0;

    // internal API: return buffer reference to field chunk data located from start_offset
    virtual std::pair<BufferView, FixedVector<bool>>
    get_chunk_buffer(FieldId field_id,
                     int64_t chunk_id,
                     int64_t start_offset,
                     int64_t length) const = 0;

    virtual std::pair<std::vector<std::string_view>, FixedVector<bool>>
    chunk_view_by_offsets(FieldId field_id,
                          int64_t chunk_id,
                          const FixedVector<int32_t>& offsets) const = 0;

    // internal API: return chunk_index in span, support scalar index only
    virtual const index::IndexBase*
    chunk_index_impl(FieldId field_id, int64_t chunk_id) const = 0;
    virtual void
    check_search(const query::Plan* plan) const = 0;

    virtual const ConcurrentVector<Timestamp>&
    get_timestamps() const = 0;

 public:
    virtual const index::IndexBase*
    chunk_index_impl(FieldId field_id,
                     std::string path,
                     int64_t chunk_id) const {
        PanicInfo(ErrorCode::NotImplemented, "not implemented");
    };

    virtual bool
    is_field_exist(FieldId field_id) const = 0;
    // calculate output[i] = Vec[seg_offsets[i]}, where Vec binds to system_type
    virtual void
    bulk_subscript(SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const = 0;

    // calculate output[i] = Vec[seg_offsets[i]}, where Vec binds to field_offset
    virtual std::unique_ptr<DataArray>
    bulk_subscript(FieldId field_id,
                   const int64_t* seg_offsets,
                   int64_t count) const = 0;

    virtual std::unique_ptr<DataArray>
    bulk_subscript(
        FieldId field_ids,
        const int64_t* seg_offsets,
        int64_t count,
        const std::vector<std::string>& dynamic_field_names) const = 0;

    virtual std::vector<SegOffset>
    search_pk(const PkType& pk, Timestamp timestamp) const = 0;

    virtual std::vector<SegOffset>
    search_pk(const PkType& pk, int64_t insert_barrier) const = 0;

 protected:
    mutable std::shared_mutex mutex_;
    // fieldID -> std::pair<num_rows, avg_size>
    std::unordered_map<FieldId, std::pair<int64_t, int64_t>>
        variable_fields_avg_size_;  // bytes;
    SkipIndex skip_index_;

    // text-indexes used to do match.
    std::unordered_map<FieldId, std::unique_ptr<index::TextMatchIndex>>
        text_indexes_;

    std::unordered_map<FieldId,
                       std::unique_ptr<index::JsonKeyStatsInvertedIndex>>
        json_indexes_;
};

}  // namespace milvus::segcore
