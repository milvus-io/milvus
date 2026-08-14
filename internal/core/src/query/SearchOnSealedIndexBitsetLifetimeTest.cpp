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
#include <string>
#include <vector>

#include "common/BitsetView.h"
#include "common/Chunk.h"
#include "common/Utils.h"
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
    const auto& valid_data = GetFieldDataRowValidData(data);
    if (valid_data.empty()) {
        return total_count;
    }
    return std::count(valid_data.begin(), valid_data.end(), true);
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

    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count);
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
    ASSERT_TRUE(vector_index->GetOffsetMapping().IsEnabled());

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
    ASSERT_TRUE(vector_index->GetOffsetMapping().IsEnabled());
    ASSERT_EQ(vector_index->GetOffsetMapping().GetValidCount(), valid_count);

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
    AssertVectorIteratorUsableAfterSearchReturns(search_result, valid_count);
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
