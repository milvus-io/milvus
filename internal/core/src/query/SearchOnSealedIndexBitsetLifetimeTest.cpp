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
#include <unordered_set>
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
#include "query/Utils.h"
#include "exec/operator/groupby/SearchGroupByOperator.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SealedIndexingRecord.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

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

TargetBitmap
MakeAdditionalFilter(int64_t total_count) {
    TargetBitmap additional_filter(total_count, false);
    for (int64_t i = 3; i < total_count; i += 5) {
        additional_filter[i] = true;
    }
    return additional_filter;
}

bool
IsFiltered(const std::vector<uint8_t>& bitset_bytes, int64_t offset) {
    return (bitset_bytes[offset >> 3] & (1U << (offset & 0x07))) != 0;
}

template <typename IsValid>
void
AssertSearchUsesCombinedLogicalFilter(SearchResult& search_result,
                                      const std::vector<uint8_t>& base_filter,
                                      const TargetBitmap& additional_filter,
                                      IsValid&& is_valid) {
    // Exercise ordinary Search through each real provider (sealed index,
    // nullable raw chunks, growing). Quotas differ from the phase-one topK.
    ASSERT_TRUE(search_result.CanSearchFilteredVectors());
    auto shared_filter =
        std::make_shared<TargetBitmap>(additional_filter.clone());
    for (int64_t remaining : {1, 2, 4}) {
        auto searched =
            search_result.SearchFilteredVectors(shared_filter, remaining);
        ASSERT_TRUE(searched);
        const auto& batch = **searched;
        EXPECT_FALSE(batch.vector_iterators_.has_value());
        EXPECT_FALSE(batch.CanSearchFilteredVectors());
        EXPECT_EQ(batch.total_nq_, 1);
        EXPECT_EQ(batch.unity_topK_, remaining);
        ASSERT_EQ(batch.seg_offsets_.size(), remaining);
        ASSERT_EQ(batch.distances_.size(), remaining);
        std::unordered_set<int64_t> seen;
        for (auto offset : batch.seg_offsets_) {
            ASSERT_GE(offset, 0);
            ASSERT_LT(offset, additional_filter.size());
            EXPECT_TRUE(seen.insert(offset).second);
            EXPECT_FALSE(IsFiltered(base_filter, offset));
            EXPECT_FALSE(additional_filter[offset]);
            EXPECT_TRUE(is_valid(offset));
        }
    }
    for (size_t i = 0; i < shared_filter->size(); ++i) {
        (*shared_filter)[i] = true;
    }
    auto empty = search_result.SearchFilteredVectors(shared_filter, 2);
    ASSERT_TRUE(empty);
    EXPECT_FALSE((**empty).vector_iterators_.has_value());
    EXPECT_EQ((**empty).seg_offsets_,
              (std::vector<int64_t>{INVALID_SEG_OFFSET, INVALID_SEG_OFFSET}));
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
    search_info.group_by_field_id_ = group_by_field;
    search_info.group_size_ = 3;
    search_info.strict_group_size_ = true;
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
BuildNullableVectorIndex(
    int64_t total_count,
    int64_t dim,
    const bool* valid_data,
    const std::vector<float>& vectors,
    const std::string& index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
    index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::COSINE;
    create_index_info.index_type = index_type;
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
        {"M", "16"},
        {"efConstruction", "100"},
    };
    index_base->BuildWithDataset(build_dataset, build_conf);
    vector_index->BuildValidData(valid_data, total_count);
    return index_base;
}

}  // namespace

TEST(StrictGroupHnswSearch, RaisesExplicitEfOnlyInPhaseTwoCopy) {
    constexpr int64_t n = 1000;
    auto schema = std::make_shared<Schema>();
    auto vector_field = schema->AddDebugField(
        "vector", DataType::VECTOR_FLOAT, kDim, knowhere::metric::COSINE);
    auto group_field = schema->AddDebugField("group", DataType::INT64);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = segcore::DataGen(schema, n);
    for (auto& column : *data.raw_->mutable_fields_data()) {
        if (column.field_id() == group_field.get()) {
            for (int64_t i = 0; i < n; ++i) {
                column.mutable_scalars()->mutable_long_data()->set_data(i,
                                                                        i % 2);
            }
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    auto vectors = MakeCompactVectors(n, kDim);
    auto valid = std::make_unique<bool[]>(n);
    std::fill_n(valid.get(), n, true);
    auto index =
        BuildNullableVectorIndex(n, kDim, valid.get(), vectors, "HNSW");
    segcore::SealedIndexingRecord record;
    record.append_field_indexing(
        vector_field,
        knowhere::metric::COSINE,
        CreateTestCacheIndex("strict-hnsw-ef-regression", std::move(index)));
    for (const auto& [ef, as_string] :
         std::vector<std::pair<int, bool>>{{0, false},
                                           {2, false},
                                           {4, false},
                                           {16, false},
                                           {2, true},
                                           {4, true},
                                           {16, true}}) {
        SCOPED_TRACE(ef);
        auto info = MakeGroupBySearchInfo(
            vector_field, group_field, knowhere::metric::COSINE);
        info.topk_ = 2;
        info.group_size_ = 5;
        info.search_params_ = knowhere::Json::object();
        if (ef != 0)
            info.search_params_["ef"] = as_string
                                            ? knowhere::Json(std::to_string(ef))
                                            : knowhere::Json(ef);
        const auto original_params = info.search_params_;
        SearchResult result;
        // VectorSearchNode normally supplies the segment row count.
        result.total_data_cnt_ = n;
        SearchOnSealedIndex(*schema,
                            record,
                            info,
                            vectors.data(),
                            nullptr,
                            1,
                            {},
                            nullptr,
                            result);
        ASSERT_TRUE(result.CanSearchFilteredVectors());
        auto filter = std::make_shared<TargetBitmap>(n, false);
        auto completed = result.SearchFilteredVectors(filter, 4);
        ASSERT_TRUE(completed);
        ASSERT_EQ((**completed).seg_offsets_.size(), 4);
        for (auto id : (**completed).seg_offsets_) EXPECT_GE(id, 0);
        const auto phase2 = StrictGroupSearchInfo(info, 4);
        if (ef != 0)
            EXPECT_EQ(phase2.search_params_["ef"],
                      as_string
                          ? knowhere::Json(std::to_string(std::max(ef, 4)))
                          : knowhere::Json(std::max(ef, 4)));
        else
            EXPECT_FALSE(phase2.search_params_.contains("ef"));
        std::vector<GroupByValueType> groups;
        std::vector<int64_t> offsets;
        std::vector<float> distances;
        std::vector<size_t> prefix;
        exec::SearchGroupBy(nullptr,
                            *result.vector_iterators_,
                            info,
                            groups,
                            *segment,
                            offsets,
                            distances,
                            prefix,
                            &result);
        EXPECT_EQ(offsets.size(), 10);
        EXPECT_EQ(info.search_params_, original_params);
        EXPECT_EQ(info.topk_, 2);
        EXPECT_EQ(info.group_size_, 5);
    }
}

TEST(StrictGroupHnswSearch, DoesNotHideInvalidExplicitEf) {
    SearchInfo original;
    original.topk_ = 2;
    for (auto ef : {knowhere::Json(0),
                    knowhere::Json(-1),
                    knowhere::Json(2.5),
                    knowhere::Json("2x"),
                    knowhere::Json(nullptr),
                    knowhere::Json(uint64_t(1) << 63)}) {
        original.search_params_ = {{"ef", ef}};
        EXPECT_EQ(StrictGroupSearchInfo(original, 4).search_params_["ef"], ef);
        EXPECT_EQ(original.search_params_["ef"], ef);
    }
}

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
    auto additional_filter = MakeAdditionalFilter(total_count);
    AssertSearchUsesCombinedLogicalFilter(
        search_result,
        logical_bitset_bytes,
        additional_filter,
        [&](int64_t offset) { return valid_data[offset]; });

    auto non_strict_search_info = search_info;
    non_strict_search_info.strict_group_size_ = false;
    SearchResult non_strict_result;
    SearchOnSealedIndex(*schema,
                        indexing_record,
                        non_strict_search_info,
                        query.data(),
                        nullptr,
                        1,
                        logical_bitset,
                        nullptr,
                        non_strict_result);
    EXPECT_FALSE(non_strict_result.CanSearchFilteredVectors());

    auto single_group_result_search_info = search_info;
    single_group_result_search_info.group_size_ = 1;
    SearchResult single_group_result;
    SearchOnSealedIndex(*schema,
                        indexing_record,
                        single_group_result_search_info,
                        query.data(),
                        nullptr,
                        1,
                        logical_bitset,
                        nullptr,
                        single_group_result);
    EXPECT_FALSE(single_group_result.CanSearchFilteredVectors());

    std::vector<float> two_queries = query;
    two_queries.insert(two_queries.end(), query.begin(), query.end());
    SearchResult multiple_query_result;
    SearchOnSealedIndex(*schema,
                        indexing_record,
                        search_info,
                        two_queries.data(),
                        nullptr,
                        2,
                        logical_bitset,
                        nullptr,
                        multiple_query_result);
    EXPECT_FALSE(multiple_query_result.CanSearchFilteredVectors());
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

    segcore::SealedIndexingRecord indexing_record;
    indexing_record.append_field_indexing(
        vector_field,
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
                        indexing_record,
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
    segcore::SealedIndexingRecord indexing_record;
    indexing_record.append_field_indexing(
        vector_field,
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
                        indexing_record,
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
    ASSERT_LT(valid_count, total_count);
    ASSERT_EQ(vector_data.valid_data_size(), total_count);

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
    auto additional_filter = MakeAdditionalFilter(total_count);
    AssertSearchUsesCombinedLogicalFilter(
        search_result,
        logical_bitset_bytes,
        additional_filter,
        [&](int64_t offset) { return vector_data.valid_data(offset); });
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
    auto additional_filter = MakeAdditionalFilter(total_count);
    AssertSearchUsesCombinedLogicalFilter(
        search_result,
        logical_bitset_bytes,
        additional_filter,
        [&](int64_t offset) { return valid_data[offset]; });
}

}  // namespace milvus::query
