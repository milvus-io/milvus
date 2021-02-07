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
#include <memory>

namespace milvus::segcore {
class SegmentSealedImpl : public SegmentSealed {
 public:
    explicit SegmentSealedImpl(SchemaPtr schema);
    void
    LoadIndex(const LoadIndexInfo& info) override;
    void
    LoadFieldData(const LoadFieldDataInfo& info) override;
    void
    DropIndex(const FieldId field_id) override;
    void
    DropFieldData(const FieldId field_id) override;

    bool
    HasIndex(FieldId field_id) const override;
    bool
    HasFieldData(FieldId field_id) const override;

 public:
    int64_t
    GetMemoryUsageInBytes() const override;

    int64_t
    get_row_count() const override;

    const Schema&
    get_schema() const override;

 public:
    int64_t
    num_chunk_index(FieldOffset field_offset) const override;

    int64_t
    num_chunk() const override;

    // return size_per_chunk for each chunk, renaming against confusion
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
    bulk_subscript(SystemFieldType system_type, const int64_t* seg_offsets, int64_t count, void* output) const override;

    // Calculate: output[i] = Vec[seg_offset[i]]
    // where Vec is determined from field_offset
    void
    bulk_subscript(FieldOffset field_offset, const int64_t* seg_offsets, int64_t count, void* output) const override;

    void
    check_search(const query::Plan* plan) const override;

 private:
    template <typename T>
    static void
    bulk_subscript_impl(const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw);

    static void
    bulk_subscript_impl(
        int64_t element_sizeof, const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw);

    void
    update_row_count(int64_t row_count) {
        if (row_count_opt_.has_value()) {
            AssertInfo(row_count_opt_.value() == row_count, "load data has different row count from other columns");
        } else {
            row_count_opt_ = row_count;
        }
    }

    void
    vector_search(int64_t vec_count,
                  query::QueryInfo query_info,
                  const void* query_data,
                  int64_t query_count,
                  const BitsetView& bitset,
                  QueryResult& output) const override;

    bool
    is_system_field_ready() const {
        return system_ready_count_ == 1;
    }

 private:
    // segment loading state
    boost::dynamic_bitset<> field_data_ready_bitset_;
    boost::dynamic_bitset<> vecindex_ready_bitset_;
    std::atomic<int> system_ready_count_ = 0;
    // segment datas
    // TODO: generate index for scalar
    std::optional<int64_t> row_count_opt_;
    std::vector<std::unique_ptr<knowhere::Index>> scalar_indexings_;
    SealedIndexingRecord vecindexs_;
    std::vector<aligned_vector<char>> field_datas_;
    aligned_vector<idx_t> row_ids_;
    SchemaPtr schema_;
};
}  // namespace milvus::segcore
