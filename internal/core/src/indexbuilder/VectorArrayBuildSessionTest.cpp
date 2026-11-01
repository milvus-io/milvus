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

#include "common/BitsetView.h"
#include "common/QueryInfo.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "exec/operator/Utils.h"
#include "index/Meta.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/VectorBuildMaterializer.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "query/CachedSearchIterator.h"
#include "storage/artifact/FileSink.h"

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
    auto dataset =
        knowhere::GenDataSet(values.size() / kDim, kDim, values.data());
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
    EXPECT_TRUE(std::all_of(
        result.seg_offsets_.begin(),
        result.seg_offsets_.end(),
        [](int64_t offset) { return offset == INVALID_SEG_OFFSET; }));
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
        auto data = storage::CreateFieldData(
            DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, nullable, kDim);
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

    void
    ExpectElementLevelHnswRoundTrip(bool nullable) {
        constexpr int64_t rows = 50;
        constexpr int64_t topk = 4;
        const Config params{
            {knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
            {knowhere::indexparam::HNSW_M, 16},
            {knowhere::indexparam::EFCONSTRUCTION, 200}};
        auto prepared = Prepare(DataType::VECTOR_ARRAY,
                                DataType::VECTOR_FLOAT,
                                nullable,
                                knowhere::IndexEnum::INDEX_HNSW,
                                rows,
                                kDim,
                                params);
        std::mt19937 rng(42);
        std::uniform_real_distribution<float> distribution(-1.0F, 1.0F);
        std::vector<VectorArray> arrays;
        arrays.reserve(rows);
        std::vector<uint8_t> validity((rows + 7) / 8, 0);
        int64_t physical_rows = 0;
        for (int64_t row = 0; row < rows; ++row) {
            // Nullable vector field data holds only the valid rows.
            if (nullable && row % 5 == 0) {
                continue;
            }
            validity[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
            const auto count = row % 3 + 1;
            std::vector<float> values(count * kDim);
            for (auto& value : values) {
                value = distribution(rng);
            }
            arrays.emplace_back(
                values.data(), count, kDim, DataType::VECTOR_FLOAT);
            physical_rows += count;
        }
        auto data = storage::CreateFieldData(
            DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, nullable, kDim);
        if (nullable) {
            data->FillFieldData(arrays.data(), validity.data(), rows, 0);
        } else {
            data->FillFieldData(arrays.data(), arrays.size());
        }
        WriteInsert(prepared, data);
        const auto stats = Publish(prepared);
        ASSERT_FALSE(stats.Files().empty());

        std::vector<float> query_values(2 * kDim);
        for (auto& value : query_values) {
            value = distribution(rng);
        }
        const auto queries = knowhere::GenDataSet(2, kDim, query_values.data());
        for (const bool mmap : {false, true}) {
            SCOPED_TRACE(mmap ? "mmap" : "heap");
            auto owner = Open(prepared, stats, mmap);
            ASSERT_EQ(owner->Count(), physical_rows);
            const auto* reader =
                dynamic_cast<const index::IVectorReader*>(owner.get());
            ASSERT_NE(reader, nullptr);
            // Null rows contribute no element, so the element domain carries
            // no validity.
            EXPECT_FALSE(reader->HasValidData());
            index::VectorSearchParams search;
            search.topk_ = topk;
            search.metric_type_ = knowhere::metric::COSINE;
            search.search_params_ =
                Config{{knowhere::meta::METRIC_TYPE, search.metric_type_},
                       {knowhere::indexparam::EF, 16}};
            SearchResult result;
            reader->Search(queries, search, nullptr, nullptr, result);
            EXPECT_EQ(result.total_nq_, 2);
            ASSERT_EQ(result.seg_offsets_.size(), 2 * topk);
            EXPECT_TRUE(std::all_of(result.seg_offsets_.begin(),
                                    result.seg_offsets_.end(),
                                    [&](int64_t offset) {
                                        return offset >= 0 &&
                                               offset < physical_rows;
                                    }));
        }
    }

    struct ElementNullableData {
        FieldDataPtr data;
        // One entry per logical element; null elements hold no vector.
        std::vector<bool> element_valid;
        std::vector<std::vector<float>> element_vectors;
        int64_t physical_elements{0};
    };

    // Rows whose index % 7 == 3 are null; the others hold row % 4 elements,
    // where element j is null when (row + j) % 3 == 0 (or always when
    // all_null_elements is set).
    static ElementNullableData
    GenElementNullable(int64_t rows, bool all_null_elements) {
        std::mt19937 rng(7);
        std::uniform_real_distribution<float> distribution(-1.0F, 1.0F);
        ElementNullableData result;
        std::vector<VectorArray> arrays;
        std::vector<uint8_t> validity((rows + 7) / 8, 0);
        for (int64_t row = 0; row < rows; ++row) {
            if (row % 7 == 3) {
                continue;
            }
            validity[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
            const auto count = static_cast<int>(row % 4);
            TargetBitmap bits(count, false);
            std::vector<float> values;
            for (int j = 0; j < count; ++j) {
                const bool valid = !all_null_elements && (row + j) % 3 != 0;
                result.element_valid.push_back(valid);
                result.element_vectors.emplace_back();
                if (!valid) {
                    continue;
                }
                bits.set(j);
                auto& vector = result.element_vectors.back();
                for (int64_t d = 0; d < kDim; ++d) {
                    vector.push_back(distribution(rng));
                }
                values.insert(values.end(), vector.begin(), vector.end());
                ++result.physical_elements;
            }
            arrays.emplace_back(values.data(),
                                count,
                                kDim,
                                DataType::VECTOR_FLOAT,
                                TargetBitmapView(bits.data(), count),
                                true);
        }
        result.data = storage::CreateFieldData(
            DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, kDim);
        result.data->FillFieldData(arrays.data(), validity.data(), rows, 0);
        return result;
    }

    PreparedBuild
    PrepareElementNullable(int64_t rows, bool disk = false) {
        Config params{{knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
                      {knowhere::indexparam::HNSW_M, 16},
                      {knowhere::indexparam::EFCONSTRUCTION, 200}};
        if (disk) {
            params = DiskParams();
            params[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
        }
        auto prepared = Prepare(DataType::VECTOR_ARRAY,
                                DataType::VECTOR_FLOAT,
                                true,
                                disk ? knowhere::IndexEnum::INDEX_DISKANN
                                     : knowhere::IndexEnum::INDEX_HNSW,
                                rows,
                                kDim,
                                params);
        prepared.file_manager_context.fieldDataMeta.field_schema
            .set_element_nullable(true);
        return prepared;
    }

    // The V1 insert-binlog fixture cannot carry element validity, so the
    // batch goes straight into the production resident materializer, builder
    // and sink; the artifact is then opened by the production loader.
    storage::ArtifactStats
    PublishElementNullable(const PreparedBuild& prepared,
                           const FieldDataPtr& data,
                           int64_t rows) {
        auto params = prepared.request.params;
        params["local_dir"] = prepared.request.staging_parent;
        VectorBuildMaterializer materializer(DataType::VECTOR_ARRAY,
                                             prepared.request.value_type,
                                             kDim,
                                             true,
                                             rows,
                                             prepared.request.family,
                                             params);
        materializer.Add(data);
        auto artifact = std::move(materializer).Build();
        storage::V1DiskSink sink(prepared.file_manager_context,
                                 prepared.request.output.storage_namespace);
        artifact->Serialize(sink);
        auto stats = sink.Finish();
        sink.ReleaseLocalStaging();
        return stats;
    }

    // Disk counterpart of PublishElementNullable.
    storage::ArtifactStats
    PublishElementNullableDisk(const PreparedBuild& prepared,
                               const FieldDataPtr& data,
                               int64_t rows) {
        auto params = prepared.request.params;
        params["local_dir"] = prepared.request.staging_parent;
        VectorDiskBuildMaterializer materializer(
            prepared.request.staging_parent,
            DataType::VECTOR_ARRAY,
            prepared.request.value_type,
            kDim,
            true,
            true,
            rows,
            prepared.request.family,
            params);
        materializer.Add(data);
        materializer.FinishPrimary();
        auto artifact = std::move(materializer).Build();
        storage::V1DiskSink sink(prepared.file_manager_context,
                                 prepared.request.output.storage_namespace);
        artifact->Serialize(sink);
        auto stats = sink.Finish();
        sink.ReleaseLocalStaging();
        return stats;
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
    query::CachedSearchIterator iterator(
        *reader, cached_queries, search, nullptr);
    SearchResult batch;
    iterator.NextBatch(search, batch);
    EXPECT_EQ(batch.total_nq_, 1);
    EXPECT_EQ(batch.unity_topK_, 2);
    ASSERT_EQ(batch.seg_offsets_.size(), 2);
    EXPECT_TRUE(std::all_of(
        batch.seg_offsets_.begin(),
        batch.seg_offsets_.end(),
        [](int64_t offset) { return offset == INVALID_SEG_OFFSET; }));
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

// A plain metric on VECTOR_ARRAY builds an element-level index, which
// knowhere serializes without embedding-list sidecars.
TEST_F(VectorArrayBuildSessionTest, HnswElementLevelListsFromBinlog) {
    ExpectElementLevelHnswRoundTrip(false);
}

// Null rows contribute no element, so a row-nullable element-level index has
// no validity and counts only the vectors of the non-null rows.
TEST_F(VectorArrayBuildSessionTest, HnswNullableElementLevelListsFromBinlog) {
    ExpectElementLevelHnswRoundTrip(true);
}

// An element-nullable field indexes its non-null elements in the logical
// element domain: null elements keep their ids but are never returned.
TEST_F(VectorArrayBuildSessionTest, HnswElementNullableElementLevel) {
    constexpr int64_t rows = 60;
    constexpr int64_t topk = 4;
    auto prepared = PrepareElementNullable(rows);
    const auto generated = GenElementNullable(rows, false);
    const auto logical = static_cast<int64_t>(generated.element_valid.size());
    ASSERT_GT(generated.physical_elements, topk);
    ASSERT_LT(generated.physical_elements, logical);
    const auto stats = PublishElementNullable(prepared, generated.data, rows);
    ASSERT_FALSE(stats.Files().empty());

    std::vector<int64_t> targets;
    for (int64_t id = 0; id < logical && targets.size() < 2; ++id) {
        if (generated.element_valid[id]) {
            targets.push_back(id);
        }
    }
    ASSERT_EQ(targets.size(), 2);
    std::vector<float> query_values;
    for (const auto id : targets) {
        const auto& vector = generated.element_vectors[id];
        query_values.insert(query_values.end(), vector.begin(), vector.end());
    }
    const auto queries = knowhere::GenDataSet(2, kDim, query_values.data());
    index::VectorSearchParams search;
    search.topk_ = topk;
    search.metric_type_ = knowhere::metric::COSINE;
    search.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, search.metric_type_},
               {knowhere::indexparam::EF, 64}};
    const auto expect_valid_elements = [&](const SearchResult& result) {
        for (const auto offset : result.seg_offsets_) {
            if (offset == INVALID_SEG_OFFSET) {
                continue;
            }
            ASSERT_GE(offset, 0);
            ASSERT_LT(offset, logical);
            EXPECT_TRUE(generated.element_valid[offset]) << offset;
        }
    };

    for (const bool mmap : {false, true}) {
        SCOPED_TRACE(mmap ? "mmap" : "heap");
        auto owner = Open(prepared, stats, mmap);
        EXPECT_EQ(owner->Count(), generated.physical_elements);
        const auto* reader =
            dynamic_cast<const index::IVectorReader*>(owner.get());
        ASSERT_NE(reader, nullptr);
        EXPECT_TRUE(reader->HasValidData());
        EXPECT_EQ(reader->ValidCount(), generated.physical_elements);
        for (int64_t id = 0; id < logical; ++id) {
            EXPECT_EQ(reader->IsRowValid(id), generated.element_valid[id])
                << id;
        }

        SearchResult result;
        reader->Search(queries, search, nullptr, nullptr, result);
        EXPECT_EQ(result.total_nq_, 2);
        ASSERT_EQ(result.seg_offsets_.size(), 2 * topk);
        expect_valid_elements(result);
        // An exact copy of an element finds that element's logical id.
        EXPECT_EQ(result.seg_offsets_[0], targets[0]);
        EXPECT_EQ(result.seg_offsets_[topk], targets[1]);

        // The filter bitset is addressed by logical element id.
        TargetBitmap filter(logical, false);
        filter.set(targets[0]);
        SearchResult filtered;
        reader->Search(queries, search, BitsetView(filter), nullptr, filtered);
        ASSERT_EQ(filtered.seg_offsets_.size(), 2 * topk);
        expect_valid_elements(filtered);
        EXPECT_TRUE(
            std::none_of(filtered.seg_offsets_.begin(),
                         filtered.seg_offsets_.end(),
                         [&](int64_t offset) { return offset == targets[0]; }));
        EXPECT_EQ(filtered.seg_offsets_[topk], targets[1]);
    }
}

// Every element null: the artifact publishes only the element-domain id map.
TEST_F(VectorArrayBuildSessionTest, HnswElementNullableAllNullElements) {
    constexpr int64_t rows = 20;
    auto prepared = PrepareElementNullable(rows);
    const auto generated = GenElementNullable(rows, true);
    const auto logical = static_cast<int64_t>(generated.element_valid.size());
    ASSERT_GT(logical, 0);
    ASSERT_EQ(generated.physical_elements, 0);
    const auto stats = PublishElementNullable(prepared, generated.data, rows);
    ASSERT_FALSE(stats.Files().empty());

    auto owner = Open(prepared, stats);
    EXPECT_EQ(owner->Count(), 0);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->HasValidData());
    EXPECT_EQ(reader->ValidCount(), 0);
    for (int64_t id = 0; id < logical; ++id) {
        EXPECT_FALSE(reader->IsRowValid(id)) << id;
    }
    const std::vector<float> values(2 * kDim, 0.1F);
    ExpectEmptySearch(*reader,
                      knowhere::GenDataSet(2, kDim, values.data()),
                      knowhere::metric::COSINE,
                      2);
}

// Rows without any element leave nothing to index; the build is skipped.
TEST_F(VectorArrayBuildSessionTest, HnswElementNullableWithoutElements) {
    constexpr int64_t rows = 4;
    auto prepared = PrepareElementNullable(rows);
    std::vector<VectorArray> arrays;
    for (int64_t row = 0; row < rows; ++row) {
        arrays.emplace_back(
            nullptr, 0, kDim, DataType::VECTOR_FLOAT, TargetBitmapView(), true);
    }
    auto data = storage::CreateFieldData(
        DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, kDim);
    const std::vector<uint8_t> validity{0x0F};
    data->FillFieldData(arrays.data(), validity.data(), rows, 0);
    try {
        PublishElementNullable(prepared, data, rows);
        FAIL() << "an index without elements must not be built";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), DataIsEmpty);
    }
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

// A plain metric on a row-nullable VECTOR_ARRAY builds an element-level
// DiskANN index: null rows contribute no element, so no validity is written
// and the index holds exactly the vectors of the non-null rows.
TEST_F(VectorArrayBuildSessionTest,
       DiskAnnNullableElementLevelListsFromBinlog) {
    constexpr int64_t rows = 60;
    constexpr int64_t topk = 4;
    auto params = DiskParams();
    params[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    auto prepared = Prepare(DataType::VECTOR_ARRAY,
                            DataType::VECTOR_FLOAT,
                            true,
                            knowhere::IndexEnum::INDEX_DISKANN,
                            rows,
                            kDim,
                            params);
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> distribution(-1.0F, 1.0F);
    std::vector<VectorArray> arrays;
    std::vector<std::vector<float>> elements;
    std::vector<uint8_t> validity((rows + 7) / 8, 0);
    for (int64_t row = 0; row < rows; ++row) {
        if (row % 5 == 0) {
            continue;
        }
        validity[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
        const auto count = row % 3 + 1;
        std::vector<float> values(count * kDim);
        for (auto& value : values) {
            value = distribution(rng);
        }
        for (int64_t j = 0; j < count; ++j) {
            elements.emplace_back(values.begin() + j * kDim,
                                  values.begin() + (j + 1) * kDim);
        }
        arrays.emplace_back(values.data(), count, kDim, DataType::VECTOR_FLOAT);
    }
    const auto physical = static_cast<int64_t>(elements.size());
    auto data = storage::CreateFieldData(
        DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, kDim);
    data->FillFieldData(arrays.data(), validity.data(), rows, 0);
    WriteInsert(prepared, data);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());

    auto owner = Open(prepared, stats);
    ASSERT_EQ(owner->Count(), physical);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_FALSE(reader->HasValidData());

    // An exact copy of an element finds that element's id.
    const std::vector<int64_t> targets{0, physical - 1};
    std::vector<float> query_values;
    for (const auto id : targets) {
        query_values.insert(
            query_values.end(), elements[id].begin(), elements[id].end());
    }
    const auto queries = knowhere::GenDataSet(2, kDim, query_values.data());
    index::VectorSearchParams search;
    search.topk_ = topk;
    search.metric_type_ = knowhere::metric::L2;
    search.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, search.metric_type_},
               {index::DISK_ANN_QUERY_LIST, topk * 4}};
    SearchResult result;
    reader->Search(queries, search, nullptr, nullptr, result);
    EXPECT_EQ(result.total_nq_, 2);
    ASSERT_EQ(result.seg_offsets_.size(), 2 * topk);
    EXPECT_TRUE(std::all_of(
        result.seg_offsets_.begin(),
        result.seg_offsets_.end(),
        [&](int64_t offset) { return offset >= 0 && offset < physical; }));
    EXPECT_EQ(result.seg_offsets_[0], targets[0]);
    EXPECT_EQ(result.seg_offsets_[topk], targets[1]);
}

// Element-nullable DiskANN: the validity is written in the logical element
// domain, so ids and the filter bitset are logical element ids.
TEST_F(VectorArrayBuildSessionTest, DiskAnnElementNullableElementLevel) {
    constexpr int64_t rows = 60;
    constexpr int64_t topk = 4;
    auto prepared = PrepareElementNullable(rows, true);
    const auto generated = GenElementNullable(rows, false);
    const auto logical = static_cast<int64_t>(generated.element_valid.size());
    ASSERT_LT(generated.physical_elements, logical);
    const auto stats =
        PublishElementNullableDisk(prepared, generated.data, rows);
    ASSERT_FALSE(stats.Files().empty());

    std::vector<int64_t> targets;
    for (int64_t id = logical - 1; id >= 0 && targets.size() < 2; --id) {
        if (generated.element_valid[id]) {
            targets.push_back(id);
        }
    }
    ASSERT_EQ(targets.size(), 2);
    std::vector<float> query_values;
    for (const auto id : targets) {
        const auto& vector = generated.element_vectors[id];
        query_values.insert(query_values.end(), vector.begin(), vector.end());
    }
    const auto queries = knowhere::GenDataSet(2, kDim, query_values.data());
    index::VectorSearchParams search;
    search.topk_ = topk;
    search.metric_type_ = knowhere::metric::L2;
    search.search_params_ =
        Config{{knowhere::meta::METRIC_TYPE, search.metric_type_},
               {index::DISK_ANN_QUERY_LIST, topk * 4}};

    auto owner = Open(prepared, stats);
    EXPECT_EQ(owner->Count(), generated.physical_elements);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->HasValidData());
    EXPECT_EQ(reader->ValidCount(), generated.physical_elements);
    for (int64_t id = 0; id < logical; ++id) {
        EXPECT_EQ(reader->IsRowValid(id), generated.element_valid[id]) << id;
    }
    const auto expect_valid_elements = [&](const SearchResult& result) {
        for (const auto offset : result.seg_offsets_) {
            if (offset == INVALID_SEG_OFFSET) {
                continue;
            }
            ASSERT_GE(offset, 0);
            ASSERT_LT(offset, logical);
            EXPECT_TRUE(generated.element_valid[offset]) << offset;
        }
    };
    SearchResult result;
    reader->Search(queries, search, nullptr, nullptr, result);
    ASSERT_EQ(result.seg_offsets_.size(), 2 * topk);
    expect_valid_elements(result);
    EXPECT_EQ(result.seg_offsets_[0], targets[0]);
    EXPECT_EQ(result.seg_offsets_[topk], targets[1]);

    TargetBitmap filter(logical, false);
    filter.set(targets[0]);
    SearchResult filtered;
    reader->Search(queries, search, BitsetView(filter), nullptr, filtered);
    ASSERT_EQ(filtered.seg_offsets_.size(), 2 * topk);
    expect_valid_elements(filtered);
    EXPECT_TRUE(
        std::none_of(filtered.seg_offsets_.begin(),
                     filtered.seg_offsets_.end(),
                     [&](int64_t offset) { return offset == targets[0]; }));
    EXPECT_EQ(filtered.seg_offsets_[topk], targets[1]);
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
