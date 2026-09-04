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

#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
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
#include "segcore/Utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"
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
                                             int64_t max_results,
                                             size_t expected_pinned_bitsets) {
    ASSERT_EQ(search_result.pinned_bitsets_.size(), expected_pinned_bitsets);
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

segcore::SealedIndexingEntry
MakeSealedIndexingEntry(const MetricType& metric_type,
                        index::CacheIndexBasePtr indexing) {
    segcore::SealedIndexingEntry entry;
    entry.metric_type_ = metric_type;
    entry.indexing_ = std::move(indexing);
    return entry;
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
                               std::vector<std::vector<char>>& chunk_buffers,
                               std::vector<int64_t> rows_per_chunk = {}) {
    std::vector<std::unique_ptr<Chunk>> chunks;
    if (rows_per_chunk.empty()) {
        rows_per_chunk.push_back(total_count);
    }

    int64_t logical_begin = 0;
    int64_t physical_begin = 0;
    for (auto chunk_rows : rows_per_chunk) {
        int64_t chunk_valid_count = 0;
        for (int64_t i = 0; i < chunk_rows; ++i) {
            if (valid_data[logical_begin + i]) {
                ++chunk_valid_count;
            }
        }

        auto null_bitmap_bytes = (chunk_rows + 7) / 8;
        auto vector_data_bytes =
            static_cast<size_t>(chunk_valid_count * dim) * sizeof(float);
        auto buffer_size = null_bitmap_bytes + vector_data_bytes;
        chunk_buffers.emplace_back(buffer_size, 0);
        char* buffer = chunk_buffers.back().data();

        int64_t chunk_physical = 0;
        for (int64_t i = 0; i < chunk_rows; ++i) {
            if (!valid_data[logical_begin + i]) {
                continue;
            }
            buffer[i >> 3] |= 1U << (i & 0x07);
            const auto* src =
                vectors.data() +
                static_cast<size_t>((physical_begin + chunk_physical) * dim);
            auto* dst =
                buffer + null_bitmap_bytes +
                static_cast<size_t>(chunk_physical * dim) * sizeof(float);
            std::memcpy(dst, src, static_cast<size_t>(dim) * sizeof(float));
            ++chunk_physical;
        }

        auto chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        chunks.emplace_back(
            std::make_unique<FixedWidthChunk>(chunk_rows,
                                              dim,
                                              buffer,
                                              buffer_size,
                                              sizeof(float),
                                              true,
                                              chunk_mmap_guard));
        logical_begin += chunk_rows;
        physical_begin += chunk_valid_count;
    }
    AssertInfo(logical_begin == total_count,
               "nullable test chunk rows do not match total rows");
    AssertInfo(physical_begin * dim == static_cast<int64_t>(vectors.size()),
               "nullable test compact vectors do not match valid rows");

    auto translator = std::make_unique<TestChunkTranslator>(
        rows_per_chunk, "", std::move(chunks));
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
    build_dataset->SetIdMapData(
        knowhere::IdMapData::FromValidData(valid_data, total_count));
    auto build_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
        {knowhere::meta::DIM, std::to_string(dim)},
        {knowhere::indexparam::NLIST, "128"},
    };
    index_base->BuildWithDataset(build_dataset, build_conf);
    return index_base;
}

struct NullableRawVectorFixture {
    SchemaPtr schema;
    FieldId vector_field;
    FieldId pk_field;
    int64_t total_count = 0;
    int64_t valid_count = 0;
    int64_t filtered_logical = 0;
    int64_t target_logical = 0;
    std::unique_ptr<bool[]> valid_data;
    std::vector<float> compact_vectors;
    std::vector<float> query;
};

NullableRawVectorFixture
MakeNullableRawVectorFixture(int64_t total_count = 1400,
                             int64_t filtered_logical = 1100,
                             int64_t target_logical = 1200) {
    NullableRawVectorFixture fixture;
    fixture.schema = std::make_shared<Schema>();
    fixture.vector_field = fixture.schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2, true);
    fixture.pk_field = fixture.schema->AddDebugField("pk", DataType::INT64);
    fixture.schema->set_primary_field_id(fixture.pk_field);
    fixture.total_count = total_count;
    fixture.filtered_logical = filtered_logical;
    fixture.target_logical = target_logical;
    fixture.valid_data = std::make_unique<bool[]>(total_count);
    fixture.query.assign(kDim, 0.0F);

    for (int64_t logical = 0; logical < total_count; ++logical) {
        const bool valid =
            (logical == filtered_logical || logical == target_logical)
                ? true
                : logical % 17 != 5;
        fixture.valid_data[logical] = valid;
        if (!valid) {
            continue;
        }
        ++fixture.valid_count;
        for (int64_t dim = 0; dim < kDim; ++dim) {
            float value = 1000.0F + static_cast<float>(logical + dim);
            if (logical == filtered_logical || logical == target_logical) {
                value = 0.0F;
            }
            fixture.compact_vectors.push_back(value);
        }
    }
    return fixture;
}

std::unique_ptr<InsertRecordProto>
MakeNullableRawVectorInsertData(const NullableRawVectorFixture& fixture) {
    std::vector<int64_t> pks(fixture.total_count);
    std::iota(pks.begin(), pks.end(), 0);

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto pk_array =
        segcore::CreateDataArrayFrom(pks.data(),
                                     nullptr,
                                     fixture.total_count,
                                     (*fixture.schema)[fixture.pk_field]);
    auto vector_array = segcore::CreateVectorDataArrayFrom(
        fixture.compact_vectors.data(),
        fixture.valid_data.get(),
        fixture.total_count,
        fixture.valid_count,
        (*fixture.schema)[fixture.vector_field]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    insert_data->mutable_fields_data()->AddAllocated(vector_array.release());
    insert_data->set_num_rows(fixture.total_count);
    return insert_data;
}

std::unique_ptr<segcore::SegmentGrowing>
MakeGrowingNullableRawVectorSegment(const NullableRawVectorFixture& fixture) {
    auto& config = segcore::SegcoreConfig::default_config();
    segcore::ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(2048);
    config.set_enable_interim_segment_index(false);

    auto segment = segcore::CreateGrowingSegment(
        fixture.schema, empty_index_meta, 0, config);
    auto insert_data = MakeNullableRawVectorInsertData(fixture);
    std::vector<idx_t> row_ids(fixture.total_count);
    std::vector<Timestamp> timestamps(fixture.total_count, 100);
    std::iota(row_ids.begin(), row_ids.end(), 0);
    auto offset = segment->PreInsert(fixture.total_count);
    segment->Insert(offset,
                    fixture.total_count,
                    row_ids.data(),
                    timestamps.data(),
                    insert_data.get());
    return segment;
}

SearchInfo
MakeNullableRawVectorSearchInfo(FieldId vector_field,
                                int64_t topk,
                                std::optional<float> radius = std::nullopt,
                                bool iterator_v2 = false) {
    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = topk;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "1"},
    };
    if (radius.has_value()) {
        search_info.search_params_[knowhere::meta::RADIUS] = radius.value();
    }
    if (iterator_v2) {
        SearchIteratorV2Info iterator_info;
        iterator_info.batch_size = topk;
        search_info.iterator_v2_info_ = iterator_info;
    }
    return search_info;
}

BitsetView
MakeSingleFilteredBitset(TargetBitmap& bitmap, int64_t filtered_logical) {
    bitmap.set(filtered_logical);
    return BitsetView(bitmap);
}

void
ExpectTargetReturnedAndFilteredSkipped(const SearchResult& result,
                                       int64_t target_logical,
                                       int64_t filtered_logical) {
    ASSERT_FALSE(result.seg_offsets_.empty());
    EXPECT_EQ(result.seg_offsets_[0], target_logical);
    EXPECT_EQ(std::find(result.seg_offsets_.begin(),
                        result.seg_offsets_.end(),
                        filtered_logical),
              result.seg_offsets_.end())
        << "filtered logical id must not be returned";
}

SearchResult
SearchGrowingNullableRawBruteForce(const NullableRawVectorFixture& fixture,
                                   SearchInfo search_info) {
    auto segment = MakeGrowingNullableRawVectorSegment(fixture);
    auto* growing_segment =
        dynamic_cast<segcore::SegmentGrowingImpl*>(segment.get());
    AssertInfo(growing_segment != nullptr, "failed to create growing segment");

    search_info.active_count_ = fixture.total_count;
    TargetBitmap filter(fixture.total_count, false);
    auto bitset = MakeSingleFilteredBitset(filter, fixture.filtered_logical);

    SearchResult result;
    SearchOnGrowing(*growing_segment,
                    search_info,
                    fixture.query.data(),
                    nullptr,
                    1,
                    MAX_TIMESTAMP,
                    bitset,
                    nullptr,
                    result);
    return result;
}

SearchResult
SearchSealedNullableRawBruteForce(const NullableRawVectorFixture& fixture,
                                  const SearchInfo& search_info,
                                  std::vector<int64_t> rows_per_chunk = {}) {
    std::vector<std::vector<char>> chunk_buffers;
    auto column =
        BuildNullableFloatVectorColumn((*fixture.schema)[fixture.vector_field],
                                       fixture.total_count,
                                       kDim,
                                       fixture.valid_data.get(),
                                       fixture.compact_vectors,
                                       chunk_buffers,
                                       std::move(rows_per_chunk));
    AssertInfo(column->GetOffsetMapping().IsEnabled(),
               "sealed nullable column must build an offset mapping");

    TargetBitmap filter(fixture.total_count, false);
    auto bitset = MakeSingleFilteredBitset(filter, fixture.filtered_logical);

    SearchResult result;
    SearchOnSealedColumn(*fixture.schema,
                         column.get(),
                         search_info,
                         std::map<std::string, std::string>{},
                         fixture.query.data(),
                         nullptr,
                         1,
                         fixture.total_count,
                         bitset,
                         nullptr,
                         result);
    return result;
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
    ASSERT_TRUE(vector_index->HasValidData());
    ASSERT_EQ(vector_index->GetIdMap().OutCount(), total_count);
    ASSERT_EQ(vector_index->GetValidCount(), valid_count);

    auto indexing_entry = MakeSealedIndexingEntry(
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
                        indexing_entry,
                        search_info,
                        query.data(),
                        nullptr,
                        1,
                        logical_bitset,
                        nullptr,
                        search_result);

    // Indexed nullable search now passes the caller's logical bitset directly
    // to Knowhere; there is no Milvus-side transformed bitset to pin.
    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count, 0);
}

TEST(SearchOnSealedIndexCachePinLifetime,
     GroupByIteratorKeepsVectorIndexPinnedUntilSearchResultDestruction) {
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
    ASSERT_TRUE(vector_index->HasValidData());
    ASSERT_EQ(vector_index->GetIdMap().OutCount(), total_count);
    ASSERT_EQ(vector_index->GetValidCount(), valid_count);

    auto cache_index = CreateTestCacheIndex(
        "nullable-vector-index-pin-lifetime", std::move(index_base));
    auto indexing_entry =
        MakeSealedIndexingEntry(knowhere::metric::COSINE, cache_index);

    std::vector<float> query(vectors.begin(), vectors.begin() + kDim);
    auto search_info = MakeGroupBySearchInfo(
        vector_field, group_by_field, knowhere::metric::COSINE);

    {
        SearchResult search_result;
        SearchOnSealedIndex(*schema,
                            indexing_entry,
                            search_info,
                            query.data(),
                            nullptr,
                            1,
                            BitsetView{},
                            nullptr,
                            search_result);

        ASSERT_TRUE(search_result.vector_iterators_.has_value());
        ASSERT_FALSE(search_result.vector_iterators_->empty());
        ASSERT_FALSE(cache_index->ManualEvictAll())
            << "the vector index must stay pinned while SearchResult owns "
               "iterators";
    }

    EXPECT_TRUE(cache_index->ManualEvictAll())
        << "the vector index pin must be released with SearchResult";
}

TEST(SearchOnSealedIndexCancellation, PinVectorIndexUsesCallerOpContext) {
    constexpr int64_t total_count = 1000;
    constexpr int64_t topk = 10;

    int64_t valid_count = 0;
    auto valid_data = MakeValidData(total_count, valid_count);
    auto vectors = MakeCompactVectors(valid_count, kDim);

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::COSINE, true);
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_field);

    auto index_base =
        BuildNullableVectorIndex(total_count, kDim, valid_data.get(), vectors);

    milvus::OpContext* observed_ctx = nullptr;
    auto indexing_entry = MakeSealedIndexingEntry(
        knowhere::metric::COSINE,
        CreateTestCacheIndex("cancellable-search-on-sealed-index",
                             std::move(index_base),
                             &observed_ctx));

    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = topk;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::COSINE;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "32"},
    };

    folly::CancellationSource source;
    milvus::OpContext op_context(source.getToken());
    SearchResult search_result;
    SearchOnSealedIndex(*schema,
                        indexing_entry,
                        search_info,
                        vectors.data(),
                        nullptr,
                        1,
                        BitsetView{},
                        &op_context,
                        search_result);

    EXPECT_EQ(observed_ctx, &op_context);
}

TEST(SearchOnSealedIndexNullableNoFilter,
     EmptyBitsetMustNotMaskCompactVectorRows) {
    constexpr int64_t total_count = 1000;
    constexpr int64_t topk = 10;

    int64_t valid_count = 0;
    auto valid_data = MakeValidData(total_count, valid_count);
    auto vectors = MakeCompactVectors(valid_count, kDim);

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::COSINE, true);
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_field);

    auto index_base =
        BuildNullableVectorIndex(total_count, kDim, valid_data.get(), vectors);
    auto* vector_index = dynamic_cast<index::VectorIndex*>(index_base.get());
    ASSERT_NE(vector_index, nullptr);
    ASSERT_TRUE(vector_index->HasValidData());
    ASSERT_EQ(vector_index->GetIdMap().OutCount(), total_count);
    ASSERT_EQ(vector_index->GetValidCount(), valid_count);

    auto indexing_entry = MakeSealedIndexingEntry(
        knowhere::metric::COSINE,
        CreateTestCacheIndex("nullable-vector-empty-bitset",
                             std::move(index_base)));

    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = topk;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::COSINE;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "32"},
    };

    SearchResult search_result;
    SearchOnSealedIndex(*schema,
                        indexing_entry,
                        search_info,
                        vectors.data(),
                        nullptr,
                        1,
                        BitsetView{},
                        nullptr,
                        search_result);

    ASSERT_EQ(search_result.seg_offsets_.size(), topk);
    auto valid_results = std::count_if(
        search_result.seg_offsets_.begin(),
        search_result.seg_offsets_.end(),
        [](int64_t offset) { return offset != INVALID_SEG_OFFSET; });
    EXPECT_GT(valid_results, 0);
}

TEST(SearchOnSealedIndexNullableIteratorNoFilter,
     EmptyBitsetMustNotMaskCompactVectorRows) {
    constexpr int64_t total_count = 1000;
    constexpr int64_t batch_size = 10;

    int64_t valid_count = 0;
    auto valid_data = MakeValidData(total_count, valid_count);
    auto vectors = MakeCompactVectors(valid_count, kDim);

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::COSINE, true);
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_field);

    auto index_base =
        BuildNullableVectorIndex(total_count, kDim, valid_data.get(), vectors);
    auto indexing_entry = MakeSealedIndexingEntry(
        knowhere::metric::COSINE,
        CreateTestCacheIndex("nullable-vector-empty-bitset-iterator",
                             std::move(index_base)));

    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = batch_size;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::COSINE;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "32"},
    };
    search_info.iterator_v2_info_ =
        SearchIteratorV2Info{.batch_size = batch_size};

    SearchResult search_result;
    SearchOnSealedIndex(*schema,
                        indexing_entry,
                        search_info,
                        vectors.data(),
                        nullptr,
                        1,
                        BitsetView{},
                        nullptr,
                        search_result);

    ASSERT_EQ(search_result.seg_offsets_.size(), batch_size);
    auto valid_results = std::count_if(
        search_result.seg_offsets_.begin(),
        search_result.seg_offsets_.end(),
        [](int64_t offset) { return offset != INVALID_SEG_OFFSET; });
    EXPECT_GT(valid_results, 0);
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

    ASSERT_EQ(search_result.resource_pins_.size(), 1);
    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count, 0);
    search_result.vector_iterators_.reset();

    // The storage reference outlives SearchOnGrowing and is released wherever
    // the SearchResult happens to die -- which is why it is shared ownership
    // of the chunk container and not a lock (shared ownership of a
    // shared_mutex is bound to the locking thread). Drop it from a different
    // thread to keep that requirement covered.
    auto pins = std::move(search_result.resource_pins_);
    std::async(std::launch::async, [pins = std::move(pins)]() mutable {
        pins.clear();
    }).get();
}

// An empty BitsetView means "no filter", not "zero rows". Clamping the
// visible-row bound to bitset.size() therefore zeroes it, and a nullable
// vector field -- whose branch resolves the bound through the offset mapping
// -- comes back with an empty result instead of every visible row.
TEST(SearchOnGrowingBitsetLifetime, NullableGrowingEmptyBitsetMeansNoFilter) {
    constexpr int64_t total_count = 512;

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2, true);
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_field);

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

    const auto& vectors = vector_data.vectors().float_vector().data();
    ASSERT_GE(vectors.size(), kDim);

    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = kTopK;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "32"},
    };
    // The plan layer froze the visible-row bound; the kernel must carry it
    // through instead of re-deriving one from the (absent) bitset.
    search_info.active_count_ = total_count;

    SearchResult search_result;
    SearchOnGrowing(*growing_segment,
                    search_info,
                    vectors.data(),
                    nullptr,
                    1,
                    MAX_TIMESTAMP,
                    BitsetView{},
                    nullptr,
                    search_result);

    auto matched = std::count_if(
        search_result.seg_offsets_.begin(),
        search_result.seg_offsets_.end(),
        [](int64_t offset) { return offset != INVALID_SEG_OFFSET; });
    EXPECT_GT(matched, 0)
        << "an empty bitset must not be read as zero visible rows";
}

TEST(SearchOnGrowingNullableRawBruteForce,
     KnnUsesLogicalBitsetAndResultIdsAcrossIdMapChunks) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info = MakeNullableRawVectorSearchInfo(fixture.vector_field, 1);
    auto result = SearchGrowingNullableRawBruteForce(fixture, search_info);

    ASSERT_EQ(result.seg_offsets_.size(), 1);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnGrowingNullableRawBruteForce,
     RangeSearchUsesLogicalBitsetAndResultIdsAcrossIdMapChunks) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info =
        MakeNullableRawVectorSearchInfo(fixture.vector_field, 2, 0.01F);
    auto result = SearchGrowingNullableRawBruteForce(fixture, search_info);

    ASSERT_EQ(result.seg_offsets_.size(), 2);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnGrowingNullableRawBruteForce,
     IteratorUsesLogicalBitsetAndResultIdsAcrossIdMapChunks) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info = MakeNullableRawVectorSearchInfo(
        fixture.vector_field, 1, std::nullopt, true);
    auto result = SearchGrowingNullableRawBruteForce(fixture, search_info);

    ASSERT_EQ(result.seg_offsets_.size(), 1);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnGrowingNullableRawBruteForce,
     VectorArrayRowSearchReturnsLogicalIds) {
    constexpr int64_t dim = 2;
    constexpr int64_t row_count = 4;

    auto schema = std::make_shared<Schema>();
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    auto vector_field =
        schema->AddDebugVectorArrayField("emb",
                                         DataType::VECTOR_FLOAT,
                                         dim,
                                         knowhere::metric::MAX_SIM_COSINE,
                                         true);
    schema->set_primary_field_id(pk_field);

    auto& config = segcore::SegcoreConfig::default_config();
    segcore::ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(false);

    auto segment =
        segcore::CreateGrowingSegment(schema, empty_index_meta, 0, config);

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(row_count);

    auto pk_data = insert_data->add_fields_data();
    pk_data->set_field_id(pk_field.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    for (int64_t pk = 0; pk < row_count; ++pk) {
        pk_data->mutable_scalars()->mutable_long_data()->add_data(pk);
    }

    auto array_data = insert_data->add_fields_data();
    array_data->set_field_id(vector_field.get());
    array_data->set_type(proto::schema::DataType::ArrayOfVector);
    array_data->add_valid_data(false);
    array_data->add_valid_data(true);
    array_data->add_valid_data(true);
    array_data->add_valid_data(true);
    array_data->mutable_vectors()->set_dim(dim);
    auto vector_array = array_data->mutable_vectors()->mutable_vector_array();
    vector_array->set_dim(dim);
    vector_array->set_element_type(proto::schema::DataType::FloatVector);

    auto empty_row = vector_array->add_data();
    empty_row->set_dim(dim);
    empty_row->mutable_float_vector();

    auto target_row = vector_array->add_data();
    target_row->set_dim(dim);
    target_row->mutable_float_vector()->add_data(1.0F);
    target_row->mutable_float_vector()->add_data(0.0F);

    auto control_row = vector_array->add_data();
    control_row->set_dim(dim);
    control_row->mutable_float_vector()->add_data(0.0F);
    control_row->mutable_float_vector()->add_data(1.0F);

    std::vector<idx_t> row_ids(row_count);
    std::iota(row_ids.begin(), row_ids.end(), 0);
    std::vector<Timestamp> timestamps(row_count, 100);
    auto reserved_offset = segment->PreInsert(row_count);
    segment->Insert(reserved_offset,
                    row_count,
                    row_ids.data(),
                    timestamps.data(),
                    insert_data.get());

    auto* growing_segment =
        dynamic_cast<segcore::SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_segment, nullptr);

    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = 2;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::MAX_SIM_COSINE;
    search_info.search_params_ = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::MAX_SIM_COSINE}};
    search_info.active_count_ = row_count;

    std::vector<float> query{1.0F, 0.0F};
    std::vector<size_t> query_offsets{0, 1};

    SearchResult result;
    SearchOnGrowing(*growing_segment,
                    search_info,
                    query.data(),
                    query_offsets.data(),
                    1,
                    MAX_TIMESTAMP,
                    BitsetView{},
                    nullptr,
                    result);

    ASSERT_EQ(result.seg_offsets_.size(), 2);
    EXPECT_EQ(result.seg_offsets_[0], 2);
    EXPECT_EQ(result.seg_offsets_[1], 3);
}

void
AssertDirectGrowingFallbackUsesACompatibleSnapshot(bool iterator_v2) {
    constexpr int64_t initial_count = 64;
    constexpr int64_t appended_count = 1;

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2);
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_field);

    auto& config = segcore::SegcoreConfig::default_config();
    segcore::ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(64);
    config.set_enable_interim_segment_index(false);
    auto segment =
        segcore::CreateGrowingSegment(schema, empty_index_meta, 0, config);
    auto initial_data = segcore::DataGen(schema, initial_count);
    auto initial_offset = segment->PreInsert(initial_count);
    segment->Insert(initial_offset,
                    initial_count,
                    initial_data.row_ids_.data(),
                    initial_data.timestamps_.data(),
                    initial_data.raw_);

    auto appended_data =
        segcore::DataGen(schema, appended_count, 43, initial_count);
    auto appended_offset = segment->PreInsert(appended_count);

    auto* growing_segment =
        dynamic_cast<segcore::SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_segment, nullptr);

    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = kTopK;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "32"},
    };
    ASSERT_EQ(search_info.active_count_, -1);
    if (iterator_v2) {
        SearchIteratorV2Info iterator_info;
        iterator_info.batch_size = kTopK;
        search_info.iterator_v2_info_ = iterator_info;
    }

    const auto& vector_data = FindFieldData(initial_data, vector_field);
    const auto& vectors = vector_data.vectors().float_vector().data();
    ASSERT_GE(vectors.size(), kDim);

    class SnapshotGate {
     public:
        void
        BlockAfterAcquire() {
            std::unique_lock lock(mutex_);
            acquired_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return released_; });
        }

        bool
        WaitUntilAcquired(std::chrono::seconds timeout) {
            std::unique_lock lock(mutex_);
            return cv_.wait_for(lock, timeout, [this] { return acquired_; });
        }

        void
        Release() {
            std::lock_guard lock(mutex_);
            released_ = true;
            cv_.notify_all();
        }

     private:
        std::mutex mutex_;
        std::condition_variable cv_;
        bool acquired_ = false;
        bool released_ = false;
    } gate;

    SetSearchOnGrowingAfterChunkSnapshotHookForTest(
        [&gate] { gate.BlockAfterAcquire(); });
    auto search_future = std::async(std::launch::async, [&] {
        auto result = std::make_shared<SearchResult>();
        SearchOnGrowing(*growing_segment,
                        search_info,
                        vectors.data(),
                        nullptr,
                        1,
                        MAX_TIMESTAMP,
                        BitsetView{},
                        nullptr,
                        *result);
        return result;
    });
    auto cleanup = folly::makeGuard([&] {
        gate.Release();
        SetSearchOnGrowingAfterChunkSnapshotHookForTest(nullptr);
    });

    ASSERT_TRUE(gate.WaitUntilAcquired(std::chrono::seconds(10)))
        << "search did not reach the post-snapshot synchronization point";

    // The search is holding a one-chunk snapshot. Publish and acknowledge one
    // more row, which creates chunk 1. The old ordering read active_count here
    // and then tried to address chunk 1 through the old snapshot. The fixed
    // ordering already froze active_count at initial_count before the hook.
    segment->Insert(appended_offset,
                    appended_count,
                    appended_data.row_ids_.data(),
                    appended_data.timestamps_.data(),
                    appended_data.raw_);
    ASSERT_EQ(segment->get_row_count(), initial_count + appended_count);

    gate.Release();
    auto search_result = search_future.get();
    ASSERT_NE(search_result, nullptr);
    EXPECT_EQ(search_result->total_nq_, 1);
    auto matched = std::count_if(
        search_result->seg_offsets_.begin(),
        search_result->seg_offsets_.end(),
        [](int64_t offset) { return offset != INVALID_SEG_OFFSET; });
    EXPECT_GT(matched, 0);
    for (auto offset : search_result->seg_offsets_) {
        if (offset != INVALID_SEG_OFFSET) {
            EXPECT_LT(offset, initial_count)
                << "the fallback bound must stay on the pre-insert prefix";
        }
    }
}

// Direct/legacy callers leave active_count_ unset. Pause immediately after the
// one-chunk snapshot is acquired, then publish and ack a row in chunk 1. These
// tests fail with the old snapshot-before-bound ordering and cover both paths
// that consume the frozen pair.
TEST(SearchOnGrowingSnapshot,
     DirectCallerInsertAfterSnapshotIsSafeForBruteForce) {
    AssertDirectGrowingFallbackUsesACompatibleSnapshot(false);
}

TEST(SearchOnGrowingSnapshot,
     DirectCallerInsertAfterSnapshotIsSafeForIteratorV2) {
    AssertDirectGrowingFallbackUsesACompatibleSnapshot(true);
}

void
AssertGrowingIndexEmptyBitsetHonorsPlannedPrefix(bool nullable) {
    constexpr int64_t initial_count = 64;
    constexpr int64_t appended_count = 32;
    SCOPED_TRACE(nullable ? "nullable" : "non-nullable");

    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2, nullable);
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "1"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(kDim)}};
    FieldIndexMeta field_index_meta(
        vector_field, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> field_map = {
        {vector_field, field_index_meta}};
    IndexMetaPtr index_meta =
        std::make_shared<CollectionIndexMeta>(100, std::move(field_map));

    auto& config = segcore::SegcoreConfig::default_config();
    segcore::ScopedSegcoreConfigRestore config_restore(config);
    segcore::InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 64;
    interim_config.nlist = 1;
    interim_config.nprobe = 1;
    interim_config.dense_vector_interim_index_type =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    segcore::ApplyInterimIndexConfigForTest(interim_config, config);

    auto segment = segcore::CreateGrowingSegment(schema, index_meta, 0, config);
    auto first = segcore::DataGen(schema,
                                  initial_count,
                                  /*seed=*/42,
                                  /*ts_offset=*/0,
                                  /*repeat_count=*/1,
                                  /*array_len=*/10,
                                  /*group_count=*/1,
                                  /*random_pk=*/false,
                                  /*random_val=*/true,
                                  /*random_valid=*/false,
                                  /*null_percent=*/10);
    auto offset = segment->PreInsert(initial_count);
    segment->Insert(offset,
                    initial_count,
                    first.row_ids_.data(),
                    first.timestamps_.data(),
                    first.raw_);

    auto* growing_segment =
        dynamic_cast<segcore::SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_segment, nullptr);
    ASSERT_TRUE(
        growing_segment->get_indexing_record().SyncDataWithIndex(vector_field));
    const auto planned_count = growing_segment->get_row_count();
    ASSERT_EQ(planned_count, initial_count);

    auto appended = segcore::DataGen(schema,
                                     appended_count,
                                     /*seed=*/84,
                                     /*ts_offset=*/initial_count,
                                     /*repeat_count=*/1,
                                     /*array_len=*/10,
                                     /*group_count=*/1,
                                     /*random_pk=*/false,
                                     /*random_val=*/true,
                                     /*random_valid=*/false,
                                     /*null_percent=*/10);
    DataArray* appended_vectors = nullptr;
    for (int i = 0; i < appended.raw_->fields_data_size(); ++i) {
        auto* field_data = appended.raw_->mutable_fields_data(i);
        if (field_data->field_id() == vector_field.get()) {
            appended_vectors = field_data;
            break;
        }
    }
    ASSERT_NE(appended_vectors, nullptr);
    auto* appended_values = appended_vectors->mutable_vectors()
                                ->mutable_float_vector()
                                ->mutable_data();
    ASSERT_GE(appended_values->size(), kDim);
    for (int i = 0; i < appended_values->size(); ++i) {
        appended_values->Set(i, 10000.0F + static_cast<float>(i % kDim));
    }
    std::vector<float> appended_query(kDim);
    std::copy_n(appended_values->begin(), kDim, appended_query.data());

    offset = segment->PreInsert(appended_count);
    segment->Insert(offset,
                    appended_count,
                    appended.row_ids_.data(),
                    appended.timestamps_.data(),
                    appended.raw_);
    ASSERT_EQ(growing_segment->get_row_count(), initial_count + appended_count);

    SearchInfo search_info;
    search_info.field_id_ = vector_field;
    search_info.topk_ = 1;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "1"},
    };
    search_info.active_count_ = planned_count;

    SearchResult search_result;
    SearchOnGrowing(*growing_segment,
                    search_info,
                    appended_query.data(),
                    nullptr,
                    1,
                    MAX_TIMESTAMP,
                    BitsetView{},
                    nullptr,
                    search_result);

    ASSERT_EQ(search_result.seg_offsets_.size(), 1);
    ASSERT_NE(search_result.seg_offsets_[0], INVALID_SEG_OFFSET);
    EXPECT_LT(search_result.seg_offsets_[0], planned_count)
        << "the growing index returned a row appended after the query's "
           "visible prefix was frozen";
}

// Freeze a query at the first acknowledged prefix, then append another batch
// before the index search starts.  The second batch is fully acknowledged here
// to make the regression deterministic; it represents the same index state a
// concurrent insert exposes before its final ack.  An empty bitset must mean
// "no predicate" inside the frozen prefix, not IDSelectorAll over the index's
// latest row count.
TEST(SearchOnGrowingBitsetLifetime,
     NullableGrowingIndexEmptyBitsetHonorsPlannedPrefix) {
    AssertGrowingIndexEmptyBitsetHonorsPlannedPrefix(true);
}

TEST(SearchOnGrowingBitsetLifetime,
     NonNullableGrowingIndexEmptyBitsetHonorsPlannedPrefix) {
    AssertGrowingIndexEmptyBitsetHonorsPlannedPrefix(false);
}

TEST(SearchOnSealedColumnNullableRawBruteForce,
     KnnUsesLogicalBitsetAndResultIds) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info = MakeNullableRawVectorSearchInfo(fixture.vector_field, 1);
    auto result = SearchSealedNullableRawBruteForce(fixture, search_info);

    ASSERT_EQ(result.seg_offsets_.size(), 1);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnSealedColumnNullableRawBruteForce,
     KnnUsesLogicalBitsetAndResultIdsAcrossPhysicalChunks) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info = MakeNullableRawVectorSearchInfo(fixture.vector_field, 1);
    auto result =
        SearchSealedNullableRawBruteForce(fixture, search_info, {700, 700});

    ASSERT_EQ(result.seg_offsets_.size(), 1);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnSealedColumnNullableRawBruteForce,
     RangeSearchUsesLogicalBitsetAndResultIds) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info =
        MakeNullableRawVectorSearchInfo(fixture.vector_field, 2, 0.01F);
    auto result = SearchSealedNullableRawBruteForce(fixture, search_info);

    ASSERT_EQ(result.seg_offsets_.size(), 2);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnSealedColumnNullableRawBruteForce,
     RangeSearchUsesLogicalBitsetAndResultIdsAcrossPhysicalChunks) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info =
        MakeNullableRawVectorSearchInfo(fixture.vector_field, 2, 0.01F);
    auto result =
        SearchSealedNullableRawBruteForce(fixture, search_info, {700, 700});

    ASSERT_EQ(result.seg_offsets_.size(), 2);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnSealedColumnNullableRawBruteForce,
     IteratorUsesLogicalBitsetAndResultIds) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info = MakeNullableRawVectorSearchInfo(
        fixture.vector_field, 1, std::nullopt, true);
    auto result = SearchSealedNullableRawBruteForce(fixture, search_info);

    ASSERT_EQ(result.seg_offsets_.size(), 1);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
}

TEST(SearchOnSealedColumnNullableRawBruteForce,
     IteratorUsesLogicalBitsetAndResultIdsAcrossPhysicalChunks) {
    auto fixture = MakeNullableRawVectorFixture();
    auto search_info = MakeNullableRawVectorSearchInfo(
        fixture.vector_field, 1, std::nullopt, true);
    auto result =
        SearchSealedNullableRawBruteForce(fixture, search_info, {700, 700});

    ASSERT_EQ(result.seg_offsets_.size(), 1);
    ExpectTargetReturnedAndFilteredSkipped(
        result, fixture.target_logical, fixture.filtered_logical);
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

    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count, 0);
}

}  // namespace milvus::query
