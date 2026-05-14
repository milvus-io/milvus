// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/BitsetView.h"
#include "common/Chunk.h"
#include "common/IndexMeta.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "index/IndexFactory.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "mmap/ChunkedColumn.h"
#include "query/SearchOnGrowing.h"
#include "query/SearchOnSealed.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SealedIndexingRecord.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus::query {
namespace {

constexpr int64_t kDim = 36;
constexpr int64_t kTopK = 15;

std::vector<uint8_t>
MakeLogicalBitsetBytes(int64_t total_count) {
    std::vector<uint8_t> logical_bitset_bytes((total_count + 7) / 8, 0);
    for (int64_t i = 0; i < total_count; i += 7) {
        logical_bitset_bytes[i >> 3] |= 1U << (i & 0x07);
    }
    return logical_bitset_bytes;
}

std::unique_ptr<bool[]>
MakeValidData(int64_t total_count, int64_t& valid_count) {
    std::unique_ptr<bool[]> valid_data(new bool[total_count]);
    valid_count = 0;
    for (int64_t i = 0; i < total_count; ++i) {
        valid_data[i] = i % 10 != 9;
        if (valid_data[i]) {
            ++valid_count;
        }
    }
    return valid_data;
}

std::vector<float>
MakeCompactVectors(int64_t valid_count, int64_t dim) {
    std::vector<float> vectors(static_cast<size_t>(valid_count * dim));
    for (size_t i = 0; i < vectors.size(); ++i) {
        vectors[i] = static_cast<float>((i % 97) + 1) / 97.0F;
    }
    return vectors;
}

SearchInfo
MakeGroupBySearchInfo(FieldId vector_field,
                      FieldId group_by_field,
                      const MetricType& metric_type) {
    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = kTopK;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = metric_type;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "32"},
    };
    search_info.group_by_field_ids_.push_back(group_by_field);
    return search_info;
}

void
AssertVectorIteratorUsableAfterSearchReturns(SearchResult& search_result,
                                             int64_t max_results) {
    ASSERT_EQ(search_result.pinned_bitsets_.size(), 1);
    ASSERT_TRUE(search_result.vector_iterators_.has_value());
    ASSERT_FALSE(search_result.vector_iterators_->empty());

    auto iterator = search_result.vector_iterators_->at(0);
    ASSERT_NE(iterator, nullptr);

    int64_t result_count = 0;
    while (iterator->HasNext() && result_count < max_results) {
        auto result = iterator->Next();
        ASSERT_TRUE(result.has_value());
        ++result_count;
    }
    ASSERT_GT(result_count, 0);
}

const DataArray&
FindFieldData(const segcore::GeneratedData& dataset, FieldId field_id) {
    for (const auto& field_data : dataset.raw_->fields_data()) {
        if (field_data.field_id() == field_id.get()) {
            return field_data;
        }
    }
    ThrowInfo(FieldIDInvalid, "field id not found: {}", field_id.get());
}

int64_t
CountValidRows(const DataArray& data, int64_t total_count) {
    if (data.valid_data_size() == 0) {
        return total_count;
    }
    return std::count(data.valid_data().begin(), data.valid_data().end(), true);
}

std::shared_ptr<ChunkedColumn>
BuildNullableFloatVectorColumn(const FieldMeta& field_meta,
                               int64_t total_count,
                               int64_t dim,
                               const bool* valid_data,
                               const std::vector<float>& vectors,
                               std::vector<std::vector<char>>& chunk_buffers) {
    std::vector<std::unique_ptr<Chunk>> chunks;
    std::vector<int64_t> num_rows_per_chunk;
    num_rows_per_chunk.push_back(total_count);

    auto null_bitmap_bytes = (total_count + 7) / 8;
    auto vector_data_bytes = vectors.size() * sizeof(float);
    auto buffer_size = null_bitmap_bytes + vector_data_bytes;
    chunk_buffers.emplace_back(buffer_size, 0);
    char* buffer = chunk_buffers.back().data();

    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            buffer[i >> 3] |= 1U << (i & 0x07);
        }
    }
    std::memcpy(buffer + null_bitmap_bytes, vectors.data(), vector_data_bytes);

    auto chunk_mmap_guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    chunks.emplace_back(std::make_unique<FixedWidthChunk>(total_count,
                                                          dim,
                                                          buffer,
                                                          buffer_size,
                                                          sizeof(float),
                                                          true,
                                                          chunk_mmap_guard));

    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "", std::move(chunks));
    auto slot =
        cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
            std::move(translator), nullptr);
    auto column = std::make_shared<ChunkedColumn>(std::move(slot), field_meta);
    column->BuildValidRowIds(nullptr);
    return column;
}

std::unique_ptr<index::IndexBase>
BuildNullableVectorIndex(int64_t total_count,
                         int64_t dim,
                         const bool* valid_data,
                         const std::vector<float>& vectors) {
    index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::COSINE;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto index_base = index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, storage::FileManagerContext());
    auto* vector_index = dynamic_cast<index::VectorIndex*>(index_base.get());
    if (vector_index == nullptr) {
        ADD_FAILURE() << "failed to create vector index";
        return index_base;
    }

    auto build_dataset =
        knowhere::GenDataSet(vectors.size() / dim, dim, vectors.data());
    auto build_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
        {knowhere::meta::DIM, std::to_string(dim)},
        {knowhere::indexparam::NLIST, "128"},
    };
    index_base->BuildWithDataset(build_dataset, build_conf);
    vector_index->BuildValidData(valid_data, total_count);
    return index_base;
}

}  // namespace

TEST(SearchOnSealedIndexBitsetLifetime,
     GroupByIteratorMustNotKeepDanglingTransformedBitset) {
    constexpr int64_t total_count = 10000;

    int64_t valid_count = 0;
    auto valid_data = MakeValidData(total_count, valid_count);
    auto vectors = MakeCompactVectors(valid_count, kDim);

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::COSINE, true);
    auto group_by_field = schema->AddDebugField("group_by", DataType::INT8);
    schema->set_primary_field_id(group_by_field);

    auto index_base =
        BuildNullableVectorIndex(total_count, kDim, valid_data.get(), vectors);
    auto* vector_index = dynamic_cast<index::VectorIndex*>(index_base.get());
    ASSERT_NE(vector_index, nullptr);
    ASSERT_TRUE(vector_index->GetOffsetMapping().IsEnabled());

    segcore::SealedIndexingRecord indexing_record;
    indexing_record.append_field_indexing(
        vector_field,
        knowhere::metric::COSINE,
        CreateTestCacheIndex("nullable-vector-bitset-lifetime",
                             std::move(index_base)));

    auto logical_bitset_bytes = MakeLogicalBitsetBytes(total_count);
    BitsetView logical_bitset(logical_bitset_bytes.data(), total_count);

    std::vector<float> query(vectors.begin(), vectors.begin() + kDim);
    auto search_info = MakeGroupBySearchInfo(
        vector_field, group_by_field, knowhere::metric::COSINE);

    SearchResult search_result;
    SearchOnSealedIndex(*schema,
                        indexing_record,
                        search_info,
                        query.data(),
                        nullptr,
                        1,
                        logical_bitset,
                        nullptr,
                        search_result);

    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count);
}

TEST(SearchOnGrowingBitsetLifetime,
     GroupByIteratorMustNotKeepDanglingTransformedBitset) {
    constexpr int64_t total_count = 512;

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2, true);
    auto group_by_field = schema->AddDebugField("group_by", DataType::INT64);
    schema->set_primary_field_id(group_by_field);

    auto dataset = segcore::DataGen(schema,
                                    total_count,
                                    /*seed=*/42,
                                    /*ts_offset=*/0,
                                    /*repeat_count=*/1,
                                    /*array_len=*/10,
                                    /*group_count=*/1,
                                    /*random_pk=*/false,
                                    /*random_val=*/true,
                                    /*random_valid=*/false,
                                    /*null_percent=*/10);
    const auto& vector_data = FindFieldData(dataset, vector_field);
    auto valid_count = CountValidRows(vector_data, total_count);
    ASSERT_GT(valid_count, 0);

    auto segment = segcore::CreateGrowingSegment(schema, empty_index_meta);
    auto reserved_offset = segment->PreInsert(total_count);
    segment->Insert(reserved_offset,
                    total_count,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto* growing_segment =
        dynamic_cast<segcore::SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_segment, nullptr);

    auto logical_bitset_bytes = MakeLogicalBitsetBytes(total_count);
    BitsetView logical_bitset(logical_bitset_bytes.data(), total_count);

    const auto& vectors = vector_data.vectors().float_vector().data();
    ASSERT_GE(vectors.size(), kDim);
    auto search_info = MakeGroupBySearchInfo(
        vector_field, group_by_field, knowhere::metric::L2);

    SearchResult search_result;
    SearchOnGrowing(*growing_segment,
                    search_info,
                    vectors.data(),
                    nullptr,
                    1,
                    MAX_TIMESTAMP,
                    logical_bitset,
                    nullptr,
                    search_result);

    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count);
}

TEST(SearchOnSealedColumnBitsetLifetime,
     GroupByIteratorMustNotKeepDanglingTransformedBitset) {
    constexpr int64_t total_count = 512;

    int64_t valid_count = 0;
    auto valid_data = MakeValidData(total_count, valid_count);
    auto vectors = MakeCompactVectors(valid_count, kDim);

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2, true);
    auto group_by_field = schema->AddDebugField("group_by", DataType::INT8);
    schema->set_primary_field_id(group_by_field);

    std::vector<std::vector<char>> chunk_buffers;
    auto column = BuildNullableFloatVectorColumn((*schema)[vector_field],
                                                 total_count,
                                                 kDim,
                                                 valid_data.get(),
                                                 vectors,
                                                 chunk_buffers);
    ASSERT_TRUE(column->GetOffsetMapping().IsEnabled());
    ASSERT_EQ(column->GetOffsetMapping().GetValidCount(), valid_count);

    auto logical_bitset_bytes = MakeLogicalBitsetBytes(total_count);
    BitsetView logical_bitset(logical_bitset_bytes.data(), total_count);

    auto search_info = MakeGroupBySearchInfo(
        vector_field, group_by_field, knowhere::metric::L2);

    SearchResult search_result;
    SearchOnSealedColumn(*schema,
                         column.get(),
                         search_info,
                         std::map<std::string, std::string>{},
                         vectors.data(),
                         nullptr,
                         1,
                         total_count,
                         logical_bitset,
                         nullptr,
                         search_result);

    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count);
}

}  // namespace milvus::query
