// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/Utils.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/test_utils/TestArtifactIO.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorMemBuilder.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index::test {
namespace {

constexpr int64_t kRows = 512;
constexpr int64_t kDim = 4;
constexpr int64_t kBinaryDim = 64;
constexpr int64_t kSparseDim = 1024;

struct Profile {
    const char* name;
    IndexType type;
    MetricType metric;
    DataType field;
    int64_t dim;
    knowhere::Json build;
    knowhere::Json search;
};

const std::vector<Profile>&
Profiles() {
    static const std::vector<Profile> profiles{
        {"FloatIdMap",
         knowhere::IndexEnum::INDEX_FAISS_IDMAP,
         "L2",
         DataType::VECTOR_FLOAT,
         kDim,
         {},
         {}},
        {"FloatIVFPQ",
         knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
         "L2",
         DataType::VECTOR_FLOAT,
         kDim,
         {{knowhere::indexparam::NLIST, 16},
          {knowhere::indexparam::M, 4},
          {knowhere::indexparam::NBITS, 8}},
         {{knowhere::indexparam::NPROBE, 4}}},
        {"FloatIVFFlat",
         knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
         "L2",
         DataType::VECTOR_FLOAT,
         kDim,
         {{knowhere::indexparam::NLIST, 16}},
         {{knowhere::indexparam::NPROBE, 4}}},
        {"FloatIVFSQ8",
         knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
         "L2",
         DataType::VECTOR_FLOAT,
         kDim,
         {{knowhere::indexparam::NLIST, 16}},
         {{knowhere::indexparam::NPROBE, 4}}},
        {"BinaryIVFFlat",
         knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
         "JACCARD",
         DataType::VECTOR_BINARY,
         kBinaryDim,
         {{knowhere::indexparam::NLIST, 16}},
         {{knowhere::indexparam::NPROBE, 4}}},
        {"BinaryIdMap",
         knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
         "JACCARD",
         DataType::VECTOR_BINARY,
         kBinaryDim,
         {},
         {}},
        {"SparseInverted",
         knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
         "IP",
         DataType::VECTOR_SPARSE_U32_F32,
         kSparseDim,
         {{knowhere::indexparam::DROP_RATIO_BUILD, 0.0}},
         {}},
        {"SparseWand",
         knowhere::IndexEnum::INDEX_SPARSE_WAND,
         "IP",
         DataType::VECTOR_SPARSE_U32_F32,
         kSparseDim,
         {{knowhere::indexparam::DROP_RATIO_BUILD, 0.0}},
         {}},
        {"FloatHNSW",
         knowhere::IndexEnum::INDEX_HNSW,
         "L2",
         DataType::VECTOR_FLOAT,
         kDim,
         {{knowhere::indexparam::M, 16},
          {knowhere::indexparam::EFCONSTRUCTION, 200}},
         {{knowhere::indexparam::EF, 128}}},
    };
    return profiles;
}

AdaptedIndexType
Adapt(const Profile& profile, int64_t rows = kRows) {
    auto params = profile.build;
    params[METRIC_TYPE] = profile.metric;
    params[DIM_KEY] = profile.dim;
    auto adapted = AdaptIndexType({
        .index_type = profile.type,
        .field_type = profile.field,
        .element_type = DataType::NONE,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = std::move(params),
    });
    adapted.params[INDEX_NUM_ROWS_KEY] = rows;
    return adapted;
}

template <typename T>
IIndexReaderBasePtr
BuildAndLoad(const AdaptedIndexType& adapted,
             const VectorBuildInput<T>& input,
             TestArtifactData& persisted,
             bool mmap = false) {
    auto builder = BuilderRegistry<VectorBuildInput<T>>::Instance().Create(
        adapted.family, adapted.params);
    if (!builder) {
        return nullptr;
    }
    auto artifact = std::move(*builder).Build(input);
    TestArtifactSink sink(persisted, storage::Generation::V1V2);
    artifact->Serialize(sink);
    sink.Finish();
    auto source = std::make_shared<TestArtifactSource>(
        persisted, storage::Generation::V1V2);
    storage::LoadOptions options;
    options.params = adapted.params;
    options.enable_mmap = mmap;
    options.mmap_dir_path = "/tmp";
    return LoaderRegistry::Instance()
        .Lookup(adapted.family)
        .Load({OpenedIndexSource{LegacyIndexSource{source, false}}, options});
}

using SparseRow = VectorBuildInput<sparse_u32_f32>::value_type;

std::vector<float>
FloatData(int64_t rows = kRows) {
    std::vector<float> values(rows * kDim);
    for (int64_t row = 0; row < rows; ++row) {
        for (int64_t dim = 0; dim < kDim; ++dim) {
            values[row * kDim + dim] =
                static_cast<float>((row * (dim * 73 + 41) + dim * 11) % 997) /
                    997.0F +
                static_cast<float>(row) / static_cast<float>(rows * 10);
        }
    }
    return values;
}

std::vector<bin1>
BinaryData() {
    std::vector<bin1> values(kRows * kBinaryDim / 8);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<bin1>((i * 113 + i / 8 * 17) & 0xff);
    }
    return values;
}

std::vector<SparseRow>
SparseData() {
    std::vector<SparseRow> rows;
    rows.reserve(kRows);
    for (int64_t row = 0; row < kRows; ++row) {
        rows.emplace_back(2);
        rows.back().set_at(0, static_cast<uint32_t>(row), 2.0F);
        rows.back().set_at(1, static_cast<uint32_t>(row + kRows), 1.0F);
    }
    return rows;
}

VectorSearchParams
SearchParams(const Profile& profile, bool range = false) {
    auto params = profile.search;
    params[METRIC_TYPE] = profile.metric;
    if (range) {
        if (profile.field == DataType::VECTOR_SPARSE_U32_F32) {
            params[knowhere::meta::RADIUS] = 0.1;
            params[knowhere::meta::RANGE_FILTER] = 0.2;
        } else {
            params[knowhere::meta::RADIUS] = 0.2;
            params[knowhere::meta::RANGE_FILTER] = 0.1;
        }
    }
    return {.search_params_ = std::move(params),
            .metric_type_ = profile.metric,
            .topk_ = 4};
}

void
CheckQuery(const Profile& profile,
           const IIndexReaderBase& base,
           const IVectorReader& reader,
           const DatasetPtr& query,
           bool range,
           int64_t rows = kRows) {
    EXPECT_EQ(base.Count(), rows);
    EXPECT_EQ(reader.Dim(), profile.dim);
    EXPECT_EQ(reader.Metric(), profile.metric);
    EXPECT_EQ(reader.KnowhereIndexType(), profile.type);
    SearchResult result;
    reader.Search(
        query, SearchParams(profile, range), BitsetView{}, nullptr, result);
    EXPECT_EQ(result.total_nq_, 1);
    EXPECT_EQ(result.unity_topK_, 4);
    ASSERT_EQ(result.seg_offsets_.size(), 4);
    ASSERT_EQ(result.distances_.size(), 4);
    if (!range) {
        EXPECT_GE(result.seg_offsets_[0], 0);
        EXPECT_LT(result.seg_offsets_[0], rows);
        if (profile.field == DataType::VECTOR_FLOAT &&
            profile.type != knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
            EXPECT_EQ(result.seg_offsets_[0], 100);
        }
    }
}

class VectorFamilyTest : public ::testing::TestWithParam<Profile> {};

TEST_P(VectorFamilyTest, QueryAndRangeAfterArtifactLoad) {
    const auto& profile = GetParam();
    auto adapted = Adapt(profile);
    TestArtifactData persisted;
    IIndexReaderBasePtr base;
    DatasetPtr query;
    std::vector<float> dense;
    std::vector<bin1> binary;
    std::vector<SparseRow> sparse;
    if (profile.field == DataType::VECTOR_FLOAT) {
        dense = FloatData();
        base = BuildAndLoad(adapted,
                            VectorBuildInput<float>{.physical_values = dense,
                                                    .logical_rows = kRows,
                                                    .physical_rows = kRows,
                                                    .dim = kDim},
                            persisted);
        query = GenDataset(1, kDim, dense.data() + 100 * kDim);
    } else if (profile.field == DataType::VECTOR_BINARY) {
        binary = BinaryData();
        base = BuildAndLoad(adapted,
                            VectorBuildInput<bin1>{.physical_values = binary,
                                                   .logical_rows = kRows,
                                                   .physical_rows = kRows,
                                                   .dim = kBinaryDim},
                            persisted);
        query = GenDataset(1, kBinaryDim, binary.data() + 100 * kBinaryDim / 8);
    } else {
        sparse = SparseData();
        base = BuildAndLoad(
            adapted,
            VectorBuildInput<sparse_u32_f32>{.physical_values = sparse,
                                             .logical_rows = kRows,
                                             .physical_rows = kRows,
                                             .dim = kSparseDim},
            persisted);
        query = GenDataset(1, kSparseDim, sparse.data() + 100);
        query->SetIsSparse(true);
    }
    ASSERT_NE(base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(base.get());
    ASSERT_NE(reader, nullptr);
    CheckQuery(profile, *base, *reader, query, false);
    if (profile.field != DataType::VECTOR_SPARSE_U32_F32) {
        CheckQuery(profile, *base, *reader, query, true);
    }
}

TEST_P(VectorFamilyTest, MmapCapabilityAndQuery) {
#ifdef __APPLE__
    GTEST_SKIP() << "faiss mapped I/O is unavailable on macOS";
#endif
    const auto& profile = GetParam();
    // The HNSW mmap path only exercised native file mapping with a large
    // payload in the legacy suite; retain that threshold here.
    const int64_t rows =
        profile.type == knowhere::IndexEnum::INDEX_HNSW ? 270000 : kRows;
    auto adapted = Adapt(profile, rows);
    TestArtifactData persisted;
    const bool native_mmap = KnowhereMmapSupported(profile.type);
    IIndexReaderBasePtr base;
    DatasetPtr query;
    std::vector<float> dense;
    std::vector<bin1> binary;
    std::vector<SparseRow> sparse;
    if (profile.field == DataType::VECTOR_FLOAT) {
        dense = FloatData(rows);
        base = BuildAndLoad(adapted,
                            VectorBuildInput<float>{.physical_values = dense,
                                                    .logical_rows = rows,
                                                    .physical_rows = rows,
                                                    .dim = kDim},
                            persisted,
                            native_mmap);
        query = GenDataset(1, kDim, dense.data() + 100 * kDim);
    } else if (profile.field == DataType::VECTOR_BINARY) {
        binary = BinaryData();
        base = BuildAndLoad(adapted,
                            VectorBuildInput<bin1>{.physical_values = binary,
                                                   .logical_rows = kRows,
                                                   .physical_rows = kRows,
                                                   .dim = kBinaryDim},
                            persisted,
                            native_mmap);
        query = GenDataset(1, kBinaryDim, binary.data() + 100 * kBinaryDim / 8);
    } else {
        sparse = SparseData();
        base = BuildAndLoad(
            adapted,
            VectorBuildInput<sparse_u32_f32>{.physical_values = sparse,
                                             .logical_rows = kRows,
                                             .physical_rows = kRows,
                                             .dim = kSparseDim},
            persisted,
            native_mmap);
        query = GenDataset(1, kSparseDim, sparse.data() + 100);
        query->SetIsSparse(true);
    }
    ASSERT_NE(base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(KnowhereMmapSupported(reader->KnowhereIndexType()), native_mmap);
    CheckQuery(profile, *base, *reader, query, false, rows);
    if (native_mmap) {
        CheckQuery(profile, *base, *reader, query, true, rows);
    }
}

TEST_P(VectorFamilyTest, RawVectorCapabilityAndLogicalIds) {
    const auto& profile = GetParam();
    auto adapted = Adapt(profile);
    TestArtifactData persisted;
    IIndexReaderBasePtr base;
    std::vector<float> dense;
    std::vector<bin1> binary;
    std::vector<SparseRow> sparse;
    if (profile.field == DataType::VECTOR_FLOAT) {
        dense = FloatData();
        base = BuildAndLoad(adapted,
                            VectorBuildInput<float>{.physical_values = dense,
                                                    .logical_rows = kRows,
                                                    .physical_rows = kRows,
                                                    .dim = kDim},
                            persisted);
    } else if (profile.field == DataType::VECTOR_BINARY) {
        binary = BinaryData();
        base = BuildAndLoad(adapted,
                            VectorBuildInput<bin1>{.physical_values = binary,
                                                   .logical_rows = kRows,
                                                   .physical_rows = kRows,
                                                   .dim = kBinaryDim},
                            persisted);
    } else {
        sparse = SparseData();
        base = BuildAndLoad(
            adapted,
            VectorBuildInput<sparse_u32_f32>{.physical_values = sparse,
                                             .logical_rows = kRows,
                                             .physical_rows = kRows,
                                             .dim = kSparseDim},
            persisted);
    }
    ASSERT_NE(base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(base->Count(), kRows);
    const std::array<int64_t, 3> ids{511, 100, 0};
    const auto id_dataset = GenIdsDataset(ids.size(), ids.data());
    if (!reader->HasRawData()) {
        if (profile.field == DataType::VECTOR_SPARSE_U32_F32) {
            EXPECT_ANY_THROW(
                static_cast<void>(reader->GetSparseVector(id_dataset)));
        } else {
            EXPECT_ANY_THROW(static_cast<void>(reader->GetVector(id_dataset)));
        }
        return;
    }
    if (profile.field == DataType::VECTOR_SPARSE_U32_F32) {
        auto got = reader->GetSparseVector(id_dataset);
        ASSERT_NE(got, nullptr);
        for (size_t i = 0; i < ids.size(); ++i) {
            ASSERT_EQ(got[i].size(), sparse[ids[i]].size());
            for (size_t j = 0; j < got[i].size(); ++j) {
                EXPECT_EQ(got[i][j].id, sparse[ids[i]][j].id);
                EXPECT_FLOAT_EQ(got[i][j].val, sparse[ids[i]][j].val);
            }
        }
    } else {
        auto got = reader->GetVector(id_dataset);
        const size_t row_bytes = profile.field == DataType::VECTOR_BINARY
                                     ? kBinaryDim / 8
                                     : kDim * sizeof(float);
        ASSERT_EQ(got.size(), ids.size() * row_bytes);
        for (size_t i = 0; i < ids.size(); ++i) {
            const auto* expected =
                profile.field == DataType::VECTOR_BINARY
                    ? static_cast<const void*>(binary.data() +
                                               ids[i] * row_bytes)
                    : static_cast<const void*>(dense.data() + ids[i] * kDim);
            EXPECT_EQ(
                std::memcmp(got.data() + i * row_bytes, expected, row_bytes),
                0);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(LegacyVectorFamilies,
                         VectorFamilyTest,
                         ::testing::ValuesIn(Profiles()),
                         [](const ::testing::TestParamInfo<Profile>& info) {
                             return std::string(info.param.name);
                         });

TEST(VectorFamilyEdgeTest, SparseExplicitAndImplicitEmptyRows) {
    for (const auto& type : {knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                             knowhere::IndexEnum::INDEX_SPARSE_WAND}) {
        Profile profile{"SparseEmpty",
                        type,
                        "IP",
                        DataType::VECTOR_SPARSE_U32_F32,
                        3,
                        {{knowhere::indexparam::DROP_RATIO_BUILD, 0.0}},
                        {}};
        auto adapted = Adapt(profile);
        adapted.params[INDEX_NUM_ROWS_KEY] = 3;
        std::array<SparseRow, 3> rows{SparseRow(2), SparseRow(0), SparseRow(1)};
        rows[0].set_at(0, 1, 1.0F);
        rows[0].set_at(1, 2, 2.0F);
        rows[2].set_at(0, 1, 0.0F);
        TestArtifactData persisted;
        auto base = BuildAndLoad(
            adapted,
            VectorBuildInput<sparse_u32_f32>{.physical_values = rows,
                                             .logical_rows = 3,
                                             .physical_rows = 3,
                                             .dim = 3},
            persisted);
        ASSERT_NE(base, nullptr) << type;
        const auto* reader = dynamic_cast<const IVectorReader*>(base.get());
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(base->Count(), 3);
        if (!reader->HasRawData()) {
            continue;
        }
        const std::array<int64_t, 3> ids{1, 2, 0};
        auto got =
            reader->GetSparseVector(GenIdsDataset(ids.size(), ids.data()));
        ASSERT_NE(got, nullptr);
        EXPECT_EQ(got[0].size(), 0);
        EXPECT_EQ(got[1].size(), rows[2].size());
        for (size_t i = 0; i < got[1].size(); ++i) {
            EXPECT_EQ(got[1][i].id, rows[2][i].id);
            EXPECT_FLOAT_EQ(got[1][i].val, rows[2][i].val);
        }
        ASSERT_EQ(got[2].size(), 2);
        EXPECT_EQ(got[2][0].id, 1);
        EXPECT_FLOAT_EQ(got[2][1].val, 2.0F);
    }
}

TEST(VectorFamilyEdgeTest, InterimIVFPQChunksBuildAddAndFilteredSearch) {
    auto values = FloatData();
    Profile profile{"InterimIVFPQ",
                    knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
                    "L2",
                    DataType::VECTOR_FLOAT,
                    kDim,
                    {{knowhere::indexparam::NLIST, 16},
                     {knowhere::indexparam::M, 4},
                     {knowhere::indexparam::NBITS, 8}},
                    {{knowhere::indexparam::NPROBE, 4}}};
    auto adapted = Adapt(profile);
    std::array<std::span<const float>, 2> chunks{
        std::span<const float>(values.data(), 256 * kDim),
        std::span<const float>(values.data() + 256 * kDim, 256 * kDim),
    };
    VectorMemBuilder<float> builder(
        DataType::NONE,
        profile.type,
        profile.metric,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        kDim,
        adapted.params);
    auto artifact = std::move(builder).Build(InterimVectorBuildInput<float>{
        .physical_chunks = chunks,
        .logical_rows = kRows,
        .physical_rows = kRows,
        .dim = kDim,
    });
    TestArtifactData persisted;
    TestArtifactSink sink(persisted, storage::Generation::V1V2);
    artifact->Serialize(sink);
    sink.Finish();
    auto source = std::make_shared<TestArtifactSource>(
        persisted, storage::Generation::V1V2);
    storage::LoadOptions options;
    options.params = adapted.params;
    auto base = LoaderRegistry::Instance()
                    .Lookup(adapted.family)
                    .Load({OpenedIndexSource{LegacyIndexSource{source, false}},
                           options});
    ASSERT_NE(base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(base->Count(), kRows);
    auto filter = BitsetType(kRows, false);
    for (int64_t row = 0; row < kRows / 2; ++row) {
        filter.set(row);
    }
    SearchResult result;
    reader->Search(GenDataset(1, kDim, values.data()),
                   SearchParams(profile),
                   BitsetView(filter),
                   nullptr,
                   result);
    ASSERT_EQ(result.seg_offsets_.size(), 4);
    for (auto row : result.seg_offsets_) {
        EXPECT_GE(row, kRows / 2);
        EXPECT_LT(row, kRows);
    }
}

TEST(VectorFamilyEdgeTest, IVFFlatCCIteratorYieldsOffsetsAndDistances) {
    auto values = FloatData();
    Profile profile{"IVFFlatCC",
                    knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
                    "L2",
                    DataType::VECTOR_FLOAT,
                    kDim,
                    {{knowhere::indexparam::NLIST, 16}},
                    {{knowhere::indexparam::NPROBE, 4}}};
    auto adapted = Adapt(profile);
    TestArtifactData persisted;
    auto base = BuildAndLoad(adapted,
                             VectorBuildInput<float>{.physical_values = values,
                                                     .logical_rows = kRows,
                                                     .physical_rows = kRows,
                                                     .dim = kDim},
                             persisted);
    ASSERT_NE(base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(base.get());
    ASSERT_NE(reader, nullptr);
    auto query = GenDataset(1, kDim, values.data() + 100 * kDim);
    auto prepared = reader->PrepareSearchParams(SearchParams(profile));
    auto iterators = reader->Iterators(query, prepared, BitsetView{}, nullptr);
    ASSERT_TRUE(iterators.has_value()) << iterators.what();
    ASSERT_EQ(iterators.value().size(), 1);
    auto& iterator = iterators.value()[0];
    size_t visited = 0;
    while (true) {
        auto has_next = iterator->HasNext();
        ASSERT_TRUE(has_next.has_value());
        if (!has_next.value()) {
            break;
        }
        auto next = iterator->Next();
        ASSERT_TRUE(next.has_value());
        const auto [offset, distance] = next.value();
        EXPECT_GE(offset, 0);
        EXPECT_LT(offset, kRows);
        EXPECT_TRUE(std::isfinite(distance));
        EXPECT_GE(distance, 0.0F);
        ++visited;
        ASSERT_LE(visited, static_cast<size_t>(kRows));
    }
    EXPECT_GT(visited, 0);
}

}  // namespace
}  // namespace milvus::index::test
