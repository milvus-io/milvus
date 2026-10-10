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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/Slice.h"
#include "common/Types.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/sparse_utils.h"

namespace milvus::indexbuilder::test {
namespace {

using Param = std::pair<knowhere::IndexType, knowhere::MetricType>;

class VectorBuildSessionRoundTripTest
    : public SourceBuildTest,
      public ::testing::WithParamInterface<Param> {};

TEST_P(VectorBuildSessionRoundTripTest, PublishedBinlogIndexCanSearch) {
    const auto& [index_type, metric] = GetParam();
    const bool binary = index_type == knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP ||
                        index_type == knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    const bool sparse = index_type == knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
                        index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND;
    const auto field_type = binary ? DataType::VECTOR_BINARY
                            : sparse ? DataType::VECTOR_SPARSE_U32_F32
                                     : DataType::VECTOR_FLOAT;
    constexpr int64_t rows = 512;
    constexpr int64_t nq = 10;
    constexpr int64_t topk = 4;
    constexpr int64_t query_offset = 1;
    const int64_t dim = binary ? 8 : sparse ? 1024 : 4;
    Config params{{knowhere::meta::METRIC_TYPE, metric}};
    Config search{{knowhere::meta::METRIC_TYPE, metric}};
    if (index_type == knowhere::IndexEnum::INDEX_HNSW) {
        params[knowhere::indexparam::HNSW_M] = 16;
        params[knowhere::indexparam::EFCONSTRUCTION] = 200;
        search[knowhere::indexparam::EF] = 200;
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        params[knowhere::indexparam::NLIST] = 16;
        params[knowhere::indexparam::M] = 4;
        params[knowhere::indexparam::NBITS] = 8;
        search[knowhere::indexparam::NPROBE] = 4;
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT ||
               index_type == knowhere::IndexEnum::INDEX_FAISS_IVFSQ8 ||
               index_type == knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
        params[knowhere::indexparam::NLIST] = 16;
        search[knowhere::indexparam::NPROBE] = 4;
    } else if (sparse) {
        params[knowhere::indexparam::DROP_RATIO_BUILD] = 0.1;
    }
    const auto prepared =
        Prepare(field_type, DataType::NONE, false, index_type, rows, dim, params);
    auto field_data = storage::CreateFieldData(
        field_type, DataType::NONE, false, dim);
    DatasetPtr queries;
    std::vector<uint8_t> binary_values;
    std::vector<float> float_values;
    std::vector<knowhere::sparse::SparseRow<SparseValueType>> sparse_values;
    // Query tensors borrow these owning columns through search.
    if (binary) {
        binary_values.resize(rows * dim / 8);
        std::default_random_engine random(42);
        for (auto& value : binary_values) {
            value = static_cast<uint8_t>(random());
        }
        field_data->FillFieldData(binary_values.data(), rows);
        queries = knowhere::GenDataSet(
            nq, dim, binary_values.data() + (dim / 8) * query_offset);
    } else if (sparse) {
        sparse_values.reserve(rows);
        for (int64_t row = 0; row < rows; ++row) {
            sparse_values.emplace_back(2);
            sparse_values.back().set_at(0, static_cast<uint32_t>(row), 2.0F);
            sparse_values.back().set_at(1, static_cast<uint32_t>(row + rows), 1.0F);
        }
        field_data->FillFieldData(sparse_values.data(), rows);
        queries = knowhere::GenDataSet(nq, dim, sparse_values.data());
        queries->SetIsSparse(true);
    } else {
        float_values.resize(rows * dim);
        for (int64_t row = 0; row < rows; ++row) {
            std::default_random_engine random(42 + row);
            std::normal_distribution<float> distribution(0, 1);
            for (int64_t col = 0; col < dim; ++col) {
                float_values[row * dim + col] = distribution(random);
            }
        }
        field_data->FillFieldData(float_values.data(), rows);
        queries = knowhere::GenDataSet(
            nq, dim, float_values.data() + dim * query_offset);
    }
    WriteInsert(prepared, field_data);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    auto owner = Open(prepared, stats);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    if (!sparse) {
        EXPECT_EQ(reader->Dim(), dim);
    }
    index::VectorSearchParams search_params;
    search_params.topk_ = topk;
    search_params.metric_type_ = metric;
    search_params.search_params_ = search;
    SearchResult result;
    reader->Search(queries, search_params, nullptr, nullptr, result);
    EXPECT_EQ(result.total_nq_, nq);
    EXPECT_EQ(result.unity_topK_, topk);
    EXPECT_EQ(result.distances_.size(), nq * topk);
    ASSERT_EQ(result.seg_offsets_.size(), nq * topk);
    if (field_type == DataType::VECTOR_FLOAT) {
        EXPECT_EQ(result.seg_offsets_[0], query_offset);
    }
}

INSTANTIATE_TEST_SUITE_P(
    KnowhereFamilies,
    VectorBuildSessionRoundTripTest,
    ::testing::Values(
        Param{knowhere::IndexEnum::INDEX_FAISS_IDMAP, knowhere::metric::L2},
        Param{knowhere::IndexEnum::INDEX_FAISS_IVFPQ, knowhere::metric::L2},
        Param{knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, knowhere::metric::L2},
        Param{knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, knowhere::metric::L2},
        Param{knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, knowhere::metric::JACCARD},
        Param{knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, knowhere::metric::JACCARD},
        Param{knowhere::IndexEnum::INDEX_HNSW, knowhere::metric::L2},
        Param{knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX, knowhere::metric::IP},
        Param{knowhere::IndexEnum::INDEX_SPARSE_WAND, knowhere::metric::IP}),
    [](const ::testing::TestParamInfo<Param>& info) {
        return info.param.first;
    });

class VectorBuildSessionSlicingTest : public SourceBuildTest {};

TEST_F(VectorBuildSessionSlicingTest, MmapLoadRestoresSlicedValidity) {
    struct SliceSizeGuard {
        SliceSizeGuard() : previous(FILE_SLICE_SIZE.exchange(64)) {}
        ~SliceSizeGuard() { FILE_SLICE_SIZE.store(previous); }
        int64_t previous;
    } slice_size;
    constexpr int64_t rows = 600;
    constexpr int64_t dim = 4;
    const auto prepared = Prepare(DataType::VECTOR_FLOAT,
                                   DataType::NONE,
                                   true,
                                   knowhere::IndexEnum::INDEX_FAISS_IDMAP,
                                   rows,
                                   dim,
                                   {{knowhere::meta::METRIC_TYPE,
                                     knowhere::metric::L2}});
    std::vector<float> values(rows * dim);
    std::vector<uint8_t> validity((rows + 7) / 8, 0);
    int64_t valid_count = 0;
    for (int64_t row = 0; row < rows; ++row) {
        if (row % 3 != 0) {
            validity[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
            ++valid_count;
        }
        for (int64_t col = 0; col < dim; ++col) {
            values[row * dim + col] = static_cast<float>(row + col);
        }
    }
    auto data = storage::CreateFieldData(
        DataType::VECTOR_FLOAT, DataType::NONE, true, dim);
    data->FillFieldData(values.data(), validity.data(), rows, 0);
    WriteInsert(prepared, data);
    const auto stats = Publish(prepared);
    const auto has_file = [&](const std::string& name) {
        return std::any_of(stats.Files().begin(),
                            stats.Files().end(),
                            [&](const auto& file) {
                                return std::filesystem::path(file.file_name)
                                           .filename() == name;
                            });
    };
    ASSERT_TRUE(has_file(INDEX_FILE_SLICE_META));
    ASSERT_TRUE(has_file("valid_data_1"));
    auto owner = Open(prepared, stats, true);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(owner->Count(), valid_count);
    EXPECT_EQ(reader->ValidCount(), valid_count);
    for (int64_t row = 0; row < rows; ++row) {
        EXPECT_EQ(reader->IsRowValid(row), row % 3 != 0) << row;
    }
}

}  // namespace
}  // namespace milvus::indexbuilder::test
