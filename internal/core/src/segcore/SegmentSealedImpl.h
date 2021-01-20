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
#include "segcore/SegmentSealed.h"
#include "SealedIndexingRecord.h"
#include <map>
#include <vector>

namespace milvus::segcore {
class SegmentSealedImpl : public SegmentSealed {
 public:
    explicit SegmentSealedImpl(SchemaPtr schema) : schema_(schema), columns_data_(schema->size()) {
    }
    void
    LoadIndex(const LoadIndexInfo& info) override;
    void
    LoadFieldData(const LoadFieldDataInfo& info) override;

 public:
    QueryResult
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_groups[],
           const Timestamp timestamps[],
           int64_t num_groups) const override;

    int64_t
    GetMemoryUsageInBytes() const override;

    int64_t
    get_row_count() const override;

    const Schema&
    get_schema() const override;

 public:
    int64_t
    num_chunk_index_safe(FieldOffset field_offset) const override;

    int64_t
    num_chunk_data() const override;

    // return chunk_size for each chunk, renaming against confusion
    int64_t
    size_per_chunk() const override;

 protected:
    // blob and row_count
    SpanBase
    chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const override;

    const knowhere::Index*
    chunk_index_impl(FieldOffset field_offset, int64_t chunk_id) const override;

    // Calculate: output[i] = Vec[seg_offset[i]],
    // where Vec is determined from field_offset
    void
    bulk_subscript(SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const override {
        Assert(is_all_ready());
        Assert(system_type == SystemFieldType::RowId);
        bulk_subscript_impl<int64_t>(row_ids_.data(), seg_offsets, count, output);
    }

    // Calculate: output[i] = Vec[seg_offset[i]]
    // where Vec is determined from field_offset
    void
    bulk_subscript(FieldOffset field_offset, const int64_t* seg_offsets, int64_t count, void* output) const override {
        Assert(is_all_ready());
        auto& field_meta = schema_->operator[](field_offset);
        Assert(field_meta.get_data_type() == DataType::INT64);
        bulk_subscript_impl<int64_t>(columns_data_[field_offset.get()].data(), seg_offsets, count, output);
    }

 private:
    template <typename T>
    static void
    bulk_subscript_impl(const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw) {
        static_assert(IsScalar<T>);
        auto src = reinterpret_cast<const T*>(src_raw);
        auto dst = reinterpret_cast<T*>(dst_raw);
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            dst[i] = offset == -1 ? -1 : src[offset];
        }
    }

    void
    update_row_count(int64_t row_count) {
        if (row_count_opt_.has_value()) {
            AssertInfo(row_count_opt_.value() == row_count, "load data has different row count from other columns");
        } else {
            row_count_opt_ = row_count;
        }
    }

    bool
    is_all_ready() const {
        // TODO: optimize here
        // NOTE: including row_ids
        return ready_count_ == schema_->size() + 1;
    }

    mutable std::shared_mutex mutex_;
    std::atomic_int ready_count_ = 0;

 private:
    // TOOD: generate index for scalar
    std::optional<int64_t> row_count_opt_;
    std::map<FieldOffset, knowhere::IndexPtr> scalar_indexings_;
    SealedIndexingRecord vec_indexings_;
    std::vector<aligned_vector<char>> columns_data_;
    aligned_vector<idx_t> row_ids_;
    SchemaPtr schema_;
};
}  // namespace milvus::segcore
