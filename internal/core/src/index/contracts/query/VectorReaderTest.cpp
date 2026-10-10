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

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/Utils.h"
#include "common/ValidityView.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/test_utils/TestArtifactIO.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index::test {
namespace {

constexpr int64_t kDim = 2;
constexpr int64_t kLogicalRows = 4;

IIndexReaderBasePtr
LoadFloatVectors(const VectorBuildInput<float>& input, bool vector_array) {
    auto adapted = AdaptIndexType({
        .index_type = vector_array ? knowhere::IndexEnum::INDEX_HNSW : "FLAT",
        .field_type =
            vector_array ? DataType::VECTOR_ARRAY : DataType::VECTOR_FLOAT,
        .element_type = vector_array ? DataType::VECTOR_FLOAT : DataType::NONE,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = {{METRIC_TYPE,
                    vector_array ? knowhere::metric::MAX_SIM
                                 : knowhere::metric::L2},
                   {DIM_KEY, kDim}},
    });
    if (vector_array) {
        adapted.params[knowhere::indexparam::M] = 16;
    }
    adapted.params["nullable"] = true;
    adapted.params[INDEX_NUM_ROWS_KEY] = input.logical_rows;
    auto builder = BuilderRegistry<VectorBuildInput<float>>::Instance().Create(
        adapted.family, adapted.params);
    if (!builder) {
        return nullptr;
    }

    auto artifact = std::move(*builder).Build(input);
    TestArtifactData persisted;
    TestArtifactSink sink(persisted, storage::Generation::V1V2);
    artifact->Serialize(sink);
    sink.Finish();

    auto source = std::make_shared<TestArtifactSource>(
        persisted, storage::Generation::V1V2);
    storage::LoadOptions options;
    options.params = adapted.params;
    auto entry = LoaderRegistry::Instance().Lookup(adapted.family);
    return entry.Load(
        {OpenedIndexSource{LegacyIndexSource{source, false}}, options});
}

IIndexReaderBasePtr
OpenFloatVectors(bool all_null) {
    const std::array<bool, kLogicalRows> valid =
        all_null ? std::array<bool, kLogicalRows>{false, false, false, false}
                 : std::array<bool, kLogicalRows>{true, false, true, true};
    const std::vector<float> values =
        all_null ? std::vector<float>{}
                 : std::vector<float>{0.0F, 0.0F, 3.0F, 3.0F, 9.0F, 9.0F};
    const VectorBuildInput<float> input{
        .physical_values = values,
        .logical_rows = kLogicalRows,
        .physical_rows = all_null ? 0 : 3,
        .dim = kDim,
        .parent_validity = ValidityView::FromExpanded(valid.data()),
    };
    return LoadFloatVectors(input, false);
}

TEST(VectorReaderContractTest, SearchAndRetrievalUseLogicalRowIds) {
    auto reader = OpenFloatVectors(false);
    ASSERT_NE(reader, nullptr);
    const auto& base = *reader;
    const auto* vectors = dynamic_cast<const IVectorReader*>(&base);
    ASSERT_NE(vectors, nullptr);

    EXPECT_EQ(base.CoordDomain(), Domain::Row);
    EXPECT_EQ(base.ValueType(), DataType::VECTOR_FLOAT);
    EXPECT_EQ(base.Count(), 3);
    EXPECT_EQ(vectors->Dim(), kDim);
    EXPECT_EQ(vectors->Metric(), "L2");
    EXPECT_FALSE(vectors->KnowhereIndexType().empty());
    const bool refine_enabled = vectors->RefineEnabled();
    EXPECT_TRUE(vectors->HasValidData());
    EXPECT_EQ(vectors->ValidCount(), 3);
    for (int64_t row = 0; row < kLogicalRows; ++row) {
        EXPECT_EQ(vectors->IsRowValid(row), row != 1) << "row=" << row;
    }

    const std::array<float, kDim> query{0.0F, 0.0F};
    const VectorSearchParams params{
        .search_params_ = knowhere::Json::object(),
        .metric_type_ = vectors->Metric(),
        .topk_ = 3,
    };
    const auto prepared = vectors->PrepareSearchParams(params);
    EXPECT_TRUE(prepared.is_object());
    const auto dataset = GenDataset(1, kDim, query.data());
    auto iterators =
        vectors->Iterators(dataset, prepared, BitsetView{}, nullptr);
    if (iterators.has_value()) {
        EXPECT_EQ(iterators.value().size(), 1);
    } else {
        EXPECT_EQ(iterators.error(), knowhere::Status::not_implemented);
    }
    const int64_t refine_id = 2;
    auto refined = vectors->CalcDistByIDs(
        dataset, BitsetView{}, &refine_id, 1, false, nullptr);
    if (refined.has_value()) {
        EXPECT_EQ(refined.value()->GetRows(), 1);
    } else {
        EXPECT_FALSE(refine_enabled);
        EXPECT_EQ(refined.error(), knowhere::Status::not_implemented);
    }
    SearchResult result;
    vectors->Search(GenDataset(1, kDim, query.data()),
                    params,
                    BitsetView{},
                    nullptr,
                    result);
    ASSERT_EQ(result.seg_offsets_.size(), 3);
    ASSERT_EQ(result.distances_.size(), 3);
    EXPECT_EQ(result.total_nq_, 1);
    EXPECT_EQ(result.unity_topK_, 3);
    EXPECT_EQ(result.seg_offsets_[0], 0);
    EXPECT_EQ(result.seg_offsets_[1], 2);
    EXPECT_EQ(result.seg_offsets_[2], 3);
    EXPECT_FLOAT_EQ(result.distances_[0], 0.0F);
    VectorSearchParams zero_topk = params;
    zero_topk.topk_ = 0;
    SearchResult invalid_result;
    EXPECT_ANY_THROW(vectors->Search(
        dataset, zero_topk, BitsetView{}, nullptr, invalid_result));
    EXPECT_ANY_THROW(vectors->Search(GenDataset(1, 1, query.data()),
                                     params,
                                     BitsetView{},
                                     nullptr,
                                     invalid_result));

    const std::array<uint8_t, 1> excluded{1U};
    SearchResult filtered;
    vectors->Search(GenDataset(1, kDim, query.data()),
                    params,
                    BitsetView(excluded.data(), kLogicalRows),
                    nullptr,
                    filtered);
    ASSERT_EQ(filtered.seg_offsets_.size(), 3);
    EXPECT_EQ(filtered.seg_offsets_[0], 2);
    EXPECT_EQ(filtered.seg_offsets_[1], 3);
    EXPECT_EQ(filtered.seg_offsets_[2], INVALID_SEG_OFFSET);

    ASSERT_TRUE(vectors->HasRawData());
    EXPECT_TRUE(vectors->GetVector(GenIdsDataset(0, nullptr)).empty());
    const int64_t logical_id = 2;
    auto raw = vectors->GetVector(GenIdsDataset(1, &logical_id));
    ASSERT_EQ(raw.size(), kDim * sizeof(float));
    std::array<float, kDim> decoded{};
    std::memcpy(decoded.data(), raw.data(), raw.size());
    EXPECT_EQ(decoded, (std::array<float, kDim>{3.0F, 3.0F}));
    EXPECT_ANY_THROW(static_cast<void>(
        vectors->GetSparseVector(GenIdsDataset(1, &logical_id))));
    EXPECT_ANY_THROW(static_cast<void>(
        vectors->GetEmbListByIds(GenIdsDataset(1, &logical_id), "L2")));
    reader.reset();
    std::array<float, kDim> after_close{};
    std::memcpy(after_close.data(), raw.data(), raw.size());
    EXPECT_EQ(after_close, (std::array<float, kDim>{3.0F, 3.0F}));
}

TEST(VectorReaderContractTest, AllNullRowsRemainInLogicalValidityDomain) {
    auto reader = OpenFloatVectors(true);
    ASSERT_NE(reader, nullptr);
    const auto* vectors = dynamic_cast<const IVectorReader*>(reader.get());
    ASSERT_NE(vectors, nullptr);
    EXPECT_EQ(reader->Count(), 0);
    EXPECT_TRUE(vectors->HasValidData());
    EXPECT_EQ(vectors->ValidCount(), 0);
    for (int64_t row = 0; row < kLogicalRows; ++row) {
        EXPECT_FALSE(vectors->IsRowValid(row));
    }
    const std::array<float, kDim> query{0.0F, 0.0F};
    const VectorSearchParams params{
        .search_params_ = knowhere::Json::object(),
        .metric_type_ = vectors->Metric(),
        .topk_ = 2,
    };
    SearchResult result;
    vectors->Search(GenDataset(1, kDim, query.data()),
                    params,
                    BitsetView{},
                    nullptr,
                    result);
    ASSERT_EQ(result.seg_offsets_.size(), 2);
    EXPECT_EQ(result.seg_offsets_[0], INVALID_SEG_OFFSET);
    EXPECT_EQ(result.seg_offsets_[1], INVALID_SEG_OFFSET);
    const int64_t invalid_id = 0;
    EXPECT_ANY_THROW(
        static_cast<void>(vectors->GetVector(GenIdsDataset(1, &invalid_id))));
    const auto iterators =
        vectors->Iterators(GenDataset(1, kDim, query.data()),
                           vectors->PrepareSearchParams(params),
                           BitsetView{},
                           nullptr);
    ASSERT_TRUE(iterators.has_value());
    ASSERT_EQ(iterators.value().size(), 1);
}

TEST(VectorReaderContractTest, EmbeddingListRetrievalKeepsTerminalOffset) {
    const std::array<bool, 3> valid{true, false, true};
    const std::array<size_t, 3> offsets{0, 2, 2};
    const std::vector<float> values{1.0F, 2.0F, 3.0F, 4.0F};
    const VectorBuildInput<float> input{
        .physical_values = values,
        .logical_rows = 3,
        .physical_rows = 2,
        .dim = kDim,
        .parent_validity = ValidityView::FromExpanded(valid.data()),
        .embedding_offsets = std::span<const size_t>(offsets),
    };
    auto reader = LoadFloatVectors(input, true);
    ASSERT_NE(reader, nullptr);
    const auto* vectors = dynamic_cast<const IVectorReader*>(reader.get());
    ASSERT_NE(vectors, nullptr);
    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
    EXPECT_EQ(vectors->ValidCount(), 2);
    EXPECT_TRUE(vectors->IsRowValid(0));
    EXPECT_FALSE(vectors->IsRowValid(1));
    EXPECT_TRUE(vectors->IsRowValid(2));

    const std::array<int64_t, 2> ids{0, 2};
    const auto [bytes, result_offsets] = vectors->GetEmbListByIds(
        GenIdsDataset(ids.size(), ids.data()), vectors->Metric());
    EXPECT_EQ(result_offsets, (std::vector<size_t>{0, 2, 2}));
    ASSERT_EQ(bytes.size(), values.size() * sizeof(float));
    EXPECT_EQ(std::memcmp(bytes.data(), values.data(), bytes.size()), 0);
    const auto [empty_bytes, empty_offsets] =
        vectors->GetEmbListByIds(GenIdsDataset(0, nullptr), vectors->Metric());
    EXPECT_TRUE(empty_bytes.empty());
    EXPECT_EQ(empty_offsets, (std::vector<size_t>{0}));
    const int64_t missing_id = 3;
    EXPECT_ANY_THROW(static_cast<void>(vectors->GetEmbListByIds(
        GenIdsDataset(1, &missing_id), vectors->Metric())));
}

TEST(VectorReaderContractTest, EmptyValidEmbeddingListsHaveLogicalIdsWithoutVectors) {
    const std::array<bool, 3> valid{true, false, true};
    const std::array<size_t, 3> offsets{0, 0, 0};
    const std::vector<float> values;
    const VectorBuildInput<float> input{
        .physical_values = values,
        .logical_rows = 3,
        .physical_rows = 0,
        .dim = kDim,
        .parent_validity = ValidityView::FromExpanded(valid.data()),
        .embedding_offsets = std::span<const size_t>(offsets),
    };
    auto reader = LoadFloatVectors(input, true);
    ASSERT_NE(reader, nullptr);
    const auto* vectors = dynamic_cast<const IVectorReader*>(reader.get());
    ASSERT_NE(vectors, nullptr);
    EXPECT_EQ(reader->Count(), 0);
    EXPECT_TRUE(vectors->HasValidData());
    EXPECT_EQ(vectors->ValidCount(), 2);
    EXPECT_TRUE(vectors->IsRowValid(0));
    EXPECT_FALSE(vectors->IsRowValid(1));
    EXPECT_TRUE(vectors->IsRowValid(2));

    const std::array<int64_t, 2> valid_ids{2, 0};
    const auto [bytes, result_offsets] = vectors->GetEmbListByIds(
        GenIdsDataset(valid_ids.size(), valid_ids.data()), vectors->Metric());
    EXPECT_TRUE(bytes.empty());
    EXPECT_EQ(result_offsets, (std::vector<size_t>{0, 0, 0}));
    const int64_t null_id = 1;
    EXPECT_ANY_THROW(static_cast<void>(vectors->GetEmbListByIds(
        GenIdsDataset(1, &null_id), vectors->Metric())));
    const int64_t out_of_range_id = 3;
    EXPECT_ANY_THROW(static_cast<void>(vectors->GetEmbListByIds(
        GenIdsDataset(1, &out_of_range_id), vectors->Metric())));
}

TEST(VectorReaderContractTest, SparseRetrievalRespectsRuntimeCapability) {
    auto adapted = AdaptIndexType({
        .index_type = knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
        .field_type = DataType::VECTOR_SPARSE_U32_F32,
        .element_type = DataType::NONE,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = {{METRIC_TYPE, "IP"}, {DIM_KEY, 8}},
    });
    adapted.params[INDEX_NUM_ROWS_KEY] = 2;
    auto builder =
        BuilderRegistry<VectorBuildInput<sparse_u32_f32>>::Instance().Create(
            adapted.family, adapted.params);
    ASSERT_NE(builder, nullptr);
    using SparseRow = VectorBuildInput<sparse_u32_f32>::value_type;
    std::array<SparseRow, 2> rows{SparseRow(1), SparseRow(1)};
    rows[0].set_at(0, 1, 2.0F);
    rows[1].set_at(0, 3, 4.0F);
    const VectorBuildInput<sparse_u32_f32> input{
        .physical_values = rows,
        .logical_rows = 2,
        .physical_rows = 2,
        .dim = 8,
    };
    auto artifact = std::move(*builder).Build(input);
    ASSERT_NE(artifact, nullptr);
    TestArtifactData persisted;
    TestArtifactSink sink(persisted, storage::Generation::V1V2);
    artifact->Serialize(sink);
    sink.Finish();
    auto source = std::make_shared<TestArtifactSource>(
        persisted, storage::Generation::V1V2);
    storage::LoadOptions options;
    options.params = adapted.params;
    auto reader =
        LoaderRegistry::Instance()
            .Lookup(adapted.family)
            .Load(
                {OpenedIndexSource{LegacyIndexSource{source, false}}, options});
    ASSERT_NE(reader, nullptr);
    const auto* vectors = dynamic_cast<const IVectorReader*>(reader.get());
    ASSERT_NE(vectors, nullptr);
    EXPECT_EQ(reader->Count(), 2);
    EXPECT_FALSE(vectors->HasValidData());
    EXPECT_TRUE(vectors->IsRowValid(0));
    EXPECT_TRUE(vectors->IsRowValid(1));
    const std::array<int64_t, 2> ids{1, 0};
    EXPECT_ANY_THROW(
        static_cast<void>(vectors->GetVector(GenIdsDataset(1, ids.data()))));
    if (vectors->HasRawData()) {
        EXPECT_FALSE(vectors->GetSparseVector(GenIdsDataset(0, nullptr)));
        auto retrieved = vectors->GetSparseVector(GenIdsDataset(2, ids.data()));
        ASSERT_NE(retrieved, nullptr);
        ASSERT_EQ(retrieved[0].size(), 1);
        ASSERT_EQ(retrieved[1].size(), 1);
        EXPECT_EQ(retrieved[0][0].id, 3);
        EXPECT_FLOAT_EQ(retrieved[0][0].val, 4.0F);
        EXPECT_EQ(retrieved[1][0].id, 1);
        EXPECT_FLOAT_EQ(retrieved[1][0].val, 2.0F);
        reader.reset();
        EXPECT_EQ(retrieved[0][0].id, 3);
    } else {
        EXPECT_ANY_THROW(static_cast<void>(
            vectors->GetSparseVector(GenIdsDataset(2, ids.data()))));
    }
}

}  // namespace
}  // namespace milvus::index::test
