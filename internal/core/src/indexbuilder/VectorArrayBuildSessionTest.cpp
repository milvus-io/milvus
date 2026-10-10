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
#include <random>
#include <string>
#include <vector>

#include "common/QueryInfo.h"
#include "common/VectorArray.h"
#include "exec/operator/Utils.h"
#include "index/Meta.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "query/CachedSearchIterator.h"

namespace milvus::indexbuilder::test {
namespace {

constexpr int64_t kDim = 4;
constexpr int64_t kEmptyRows = 8;

Config
HnswParams() {
    return {{knowhere::meta::METRIC_TYPE, knowhere::metric::MAX_SIM},
            {knowhere::indexparam::HNSW_M, 16},
            {knowhere::indexparam::EFCONSTRUCTION, 200}};
}

Config
DiskParams() {
    return {{knowhere::meta::METRIC_TYPE, knowhere::metric::MAX_SIM_L2},
            {index::DISK_ANN_MAX_DEGREE, 24},
            {index::DISK_ANN_SEARCH_LIST_SIZE, 56},
            {index::DISK_ANN_PQ_CODE_BUDGET, 0.001},
            {index::DISK_ANN_BUILD_DRAM_BUDGET, 2},
            {index::DISK_ANN_BUILD_THREAD_NUM, 2},
            {index::DISK_ANN_LOAD_THREAD_NUM, 2},
            {index::DISK_ANN_SEARCH_CACHE_BUDGET, 0.0}};
}

knowhere::DataSetPtr
EmbeddingQueries(const std::vector<float>& values,
                 const std::vector<size_t>& offsets) {
    auto dataset = knowhere::GenDataSet(values.size() / kDim, kDim, values.data());
    dataset->Set(knowhere::meta::EMB_LIST_OFFSET, offsets.data());
    const auto count = static_cast<int64_t>(offsets.size() - 1);
    dataset->Set(knowhere::meta::EMB_LIST_COUNT, count);
    dataset->Set(knowhere::meta::NQ, count);
    return dataset;
}

void
ExpectEmptyIterators(const index::IVectorReader& reader,
                     const DatasetPtr& queries,
                     const MetricType& metric,
                     size_t count) {
    const knowhere::Json config{{knowhere::meta::METRIC_TYPE, metric},
                                {knowhere::indexparam::EF, 10}};
    auto iterators = reader.Iterators(queries, config, nullptr, nullptr);
    ASSERT_TRUE(iterators.has_value()) << iterators.what();
    ASSERT_EQ(iterators.value().size(), count);
    for (const auto& iterator : iterators.value()) {
        const auto has_next = iterator->HasNext();
        ASSERT_TRUE(has_next.has_value()) << has_next.what();
        EXPECT_FALSE(has_next.value());
    }
}

void
ExpectEmptySearch(const index::IVectorReader& reader,
                  const DatasetPtr& queries,
                  const MetricType& metric,
                  int64_t count) {
    index::VectorSearchParams params;
    params.topk_ = 3;
    params.metric_type_ = metric;
    params.search_params_ = Config{{knowhere::meta::METRIC_TYPE, metric}};
    SearchResult result;
    reader.Search(queries, params, nullptr, nullptr, result);
    EXPECT_EQ(result.total_nq_, count);
    EXPECT_EQ(result.seg_offsets_.size(), count * params.topk_);
    EXPECT_TRUE(std::all_of(result.seg_offsets_.begin(),
                            result.seg_offsets_.end(),
                            [](int64_t offset) {
                                return offset == INVALID_SEG_OFFSET;
                            }));
}

class VectorArrayBuildSessionTest : public SourceBuildTest {
 protected:
    PreparedBuild
    PrepareEmpty(bool nullable, bool disk) {
        auto prepared = Prepare(DataType::VECTOR_ARRAY,
                                 DataType::VECTOR_FLOAT,
                                 nullable,
                                 disk ? knowhere::IndexEnum::INDEX_DISKANN
                                      : knowhere::IndexEnum::INDEX_HNSW,
                                 kEmptyRows,
                                 kDim,
                                 disk ? DiskParams() : HnswParams());
        auto data = storage::CreateFieldData(DataType::VECTOR_ARRAY,
                                             DataType::VECTOR_FLOAT,
                                             nullable,
                                             kDim);
        std::vector<VectorArray> arrays;
        arrays.reserve(kEmptyRows);
        for (int64_t row = 0; row < kEmptyRows; ++row) {
            arrays.emplace_back(nullptr, 0, kDim, DataType::VECTOR_FLOAT);
        }
        if (nullable) {
            const std::vector<uint8_t> validity((kEmptyRows + 7) / 8, 0);
            data->FillFieldData(arrays.data(), validity.data(), kEmptyRows, 0);
        } else {
            data->FillFieldData(arrays.data(), arrays.size());
        }
        WriteInsert(prepared, data);
        return prepared;
    }
};

TEST_F(VectorArrayBuildSessionTest, HnswAllNullParentsFromBinlog) {
    const auto prepared = PrepareEmpty(true, false);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    auto owner = Open(prepared, stats);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(owner->Count(), 0);
    EXPECT_TRUE(reader->HasValidData());
    EXPECT_EQ(reader->ValidCount(), 0);
    EXPECT_TRUE(reader->HasRawData());
    EXPECT_FALSE(reader->RefineEnabled());
    for (int64_t row = 0; row < kEmptyRows; ++row) {
        EXPECT_FALSE(reader->IsRowValid(row));
    }

    const std::vector<float> ordinary_values(2 * kDim, 0.1F);
    auto ordinary = knowhere::GenDataSet(2, kDim, ordinary_values.data());
    ExpectEmptyIterators(*reader, ordinary, knowhere::metric::L2, 2);

    const std::vector<float> values(3 * kDim, 0.1F);
    const std::vector<size_t> offsets{0, 2, 3, 3, 3};
    const auto queries = EmbeddingQueries(values, offsets);
    ExpectEmptyIterators(*reader, queries, knowhere::metric::L2, 4);
    ExpectEmptySearch(*reader, queries, knowhere::metric::MAX_SIM, 4);
    const std::vector<float> single_values(kDim, 0.1F);
    const std::vector<size_t> single_offsets{0, 1};
    ExpectEmptySearch(*reader,
                       EmbeddingQueries(single_values, single_offsets),
                       knowhere::metric::MAX_SIM,
                       1);

    SearchInfo search;
    search.topk_ = 3;
    search.metric_type_ = knowhere::metric::MAX_SIM;
    search.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, search.metric_type_}};
    search.iterative_filter_execution = true;
    SearchResult iterative;
    EXPECT_TRUE(exec::PrepareVectorIteratorsFromIndex(
        search, 4, queries, iterative, nullptr, *reader));
    EXPECT_EQ(iterative.total_nq_, 4);
    EXPECT_EQ(iterative.unity_topK_, search.topk_);

    search.iterative_filter_execution = false;
    search.iterator_v2_info_ = SearchIteratorV2Info{"", 2};
    const std::vector<size_t> cached_offsets{0, 3};
    const auto cached_queries = EmbeddingQueries(values, cached_offsets);
    query::CachedSearchIterator iterator(*reader, cached_queries, search, nullptr);
    SearchResult batch;
    iterator.NextBatch(search, batch);
    EXPECT_EQ(batch.total_nq_, 1);
    EXPECT_EQ(batch.unity_topK_, 2);
    ASSERT_EQ(batch.seg_offsets_.size(), 2);
    EXPECT_TRUE(std::all_of(batch.seg_offsets_.begin(),
                            batch.seg_offsets_.end(),
                            [](int64_t offset) {
                                return offset == INVALID_SEG_OFFSET;
                            }));
}

TEST_F(VectorArrayBuildSessionTest, HnswValidEmptyListsFromBinlog) {
    const auto prepared = PrepareEmpty(false, false);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    auto owner = Open(prepared, stats);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(owner->Count(), 0);
    EXPECT_FALSE(reader->HasValidData());
    EXPECT_TRUE(reader->HasRawData());

    std::vector<int64_t> ids{0, 3, 7};
    const auto [raw, offsets] = reader->GetEmbListByIds(
        GenIdsDataset(ids.size(), ids.data()), knowhere::metric::MAX_SIM);
    EXPECT_TRUE(raw.empty());
    EXPECT_EQ(offsets, (std::vector<size_t>{0, 0, 0, 0}));

    const std::vector<float> ordinary_values(2 * kDim, 0.1F);
    const auto ordinary = knowhere::GenDataSet(2, kDim, ordinary_values.data());
    ExpectEmptyIterators(*reader, ordinary, knowhere::metric::L2, 2);
    const std::vector<float> values(3 * kDim, 0.1F);
    const std::vector<size_t> query_offsets{0, 2, 3, 3, 3};
    const auto queries = EmbeddingQueries(values, query_offsets);
    ExpectEmptyIterators(*reader, queries, knowhere::metric::MAX_SIM, 4);
    ExpectEmptySearch(*reader, queries, knowhere::metric::MAX_SIM, 4);
    const std::vector<size_t> single_offsets{0, 1};
    const std::vector<float> single_values(kDim, 0.1F);
    ExpectEmptySearch(*reader,
                       EmbeddingQueries(single_values, single_offsets),
                       knowhere::metric::MAX_SIM,
                       1);
}

#ifdef BUILD_DISK_ANN
TEST_F(VectorArrayBuildSessionTest, DiskAnnNonemptyListsFromBinlog) {
    constexpr int64_t rows = 100;
    constexpr int64_t topk = 4;
    auto prepared = Prepare(DataType::VECTOR_ARRAY,
                             DataType::VECTOR_FLOAT,
                             false,
                             knowhere::IndexEnum::INDEX_DISKANN,
                             rows,
                             kDim,
                             DiskParams());
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> distribution(-1.0F, 1.0F);
    std::vector<VectorArray> arrays;
    arrays.reserve(rows);
    int64_t physical_rows = 0;
    for (int64_t row = 0; row < rows; ++row) {
        const auto count = row % 5 + 1;
        std::vector<float> values(count * kDim);
        for (auto& value : values) {
            value = distribution(rng);
        }
        arrays.emplace_back(values.data(), count, kDim, DataType::VECTOR_FLOAT);
        physical_rows += count;
    }
    auto data = storage::CreateFieldData(
        DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, false, kDim);
    data->FillFieldData(arrays.data(), arrays.size());
    WriteInsert(prepared, data);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    EXPECT_GT(stats.MemSize(), 0);
    auto owner = Open(prepared, stats);
    ASSERT_EQ(owner->Count(), physical_rows);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    std::vector<float> values(5 * kDim);
    for (auto& value : values) {
        value = distribution(rng);
    }
    const std::vector<size_t> offsets{0, 3, 5};
    const auto queries = EmbeddingQueries(values, offsets);
    index::VectorSearchParams params;
    params.topk_ = topk;
    params.metric_type_ = knowhere::metric::MAX_SIM_L2;
    params.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, params.metric_type_},
               {index::DISK_ANN_QUERY_LIST, topk * 2}};
    SearchResult result;
    reader->Search(queries, params, nullptr, nullptr, result);
    EXPECT_EQ(result.total_nq_, 2);
    EXPECT_EQ(result.distances_.size(), 2 * topk);
}

TEST_F(VectorArrayBuildSessionTest, DiskAnnAllNullParentsFromBinlog) {
    const auto prepared = PrepareEmpty(true, true);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    auto owner = Open(prepared, stats);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(owner->Count(), 0);
    EXPECT_TRUE(reader->HasValidData());
    EXPECT_EQ(reader->ValidCount(), 0);
    EXPECT_TRUE(reader->HasRawData());
    EXPECT_FALSE(reader->RefineEnabled());
    const std::vector<float> values(2 * kDim, 0.1F);
    const std::vector<size_t> offsets{0, 1, 2};
    const auto queries = EmbeddingQueries(values, offsets);
    ExpectEmptyIterators(*reader, queries, knowhere::metric::MAX_SIM_L2, 2);
    ExpectEmptySearch(*reader, queries, knowhere::metric::MAX_SIM_L2, 2);
}
#endif

}  // namespace
}  // namespace milvus::indexbuilder::test
