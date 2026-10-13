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

#pragma once

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/Utils.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/test_utils/TestArtifactIO.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorDiskBuildFileManager.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index::test {

// One physical dataset serves the resident and prepared-file input channels.
// The two interleaved categories exceed Faiss HNSW's partition merge threshold;
// reversed IDs also exercise the persisted partition-to-public-ID mapping.
class VectorSideInputFixture : public ::testing::Test {
 protected:
    static constexpr int64_t kRows = 1024;
    static constexpr int64_t kDim = 4;
    static constexpr int64_t kFieldId = 203;

    struct PhysicalInput {
        std::vector<float> values;
        std::array<std::vector<uint32_t>, 2> categories;

        PhysicalInput() {
            values.reserve(kRows * kDim);
            for (int64_t row = 0; row < kRows; ++row) {
                for (int64_t col = 0; col < kDim; ++col) {
                    values.push_back(static_cast<float>(row * (col + 1)));
                }
            }
            for (int64_t row = kRows - 1; row >= 0; --row) {
                categories[row % 2 == 1 ? 0 : 1].push_back(
                    static_cast<uint32_t>(row));
            }
        }

        std::array<VectorScalarCategoryGroup, 2>
        CategoryViews() const {
            return {VectorScalarCategoryGroup{categories[0]},
                    VectorScalarCategoryGroup{categories[1]}};
        }
    };

    static OptFieldT
    OptionalFields() {
        return {{kFieldId,
                 std::make_tuple("tenant_id",
                                 DataType::INT64,
                                 DataType::NONE,
                                 std::vector<std::string>{})}};
    }

    static AdaptedIndexType
    AdaptedParams(const std::string& index_type) {
        Config params = {{METRIC_TYPE, "L2"},
                         {DIM_KEY, kDim},
                         {INDEX_NUM_ROWS_KEY, kRows},
                         {PARTITION_KEY_ISOLATION_KEY, true},
                         {VEC_OPT_FIELDS, OptionalFields()},
                         {"mv_build_minimum_category_size", 128}};
        if (index_type == knowhere::IndexEnum::INDEX_DISKANN) {
            params[DISK_ANN_MAX_DEGREE] = "24";
            params[DISK_ANN_SEARCH_LIST_SIZE] = "56";
            params[DISK_ANN_PQ_CODE_BUDGET] = "0.001";
            params[DISK_ANN_BUILD_DRAM_BUDGET] = "2";
            params[DISK_ANN_BUILD_THREAD_NUM] = "2";
        } else {
            params[knowhere::indexparam::M] = 16;
            params[knowhere::indexparam::EFCONSTRUCTION] = 96;
        }
        return AdaptIndexType({
            .index_type = index_type,
            .field_type = DataType::VECTOR_FLOAT,
            .element_type = DataType::NONE,
            .index_engine_version =
                knowhere::Version::GetCurrentVersion().VersionNumber(),
            .params = std::move(params),
        });
    }

    static void
    ExpectForwardedParams(const Config& params) {
        EXPECT_EQ(params.at(PARTITION_KEY_ISOLATION_KEY), true);
        EXPECT_EQ(params.at(VEC_OPT_FIELDS).get<OptFieldT>(), OptionalFields());
        EXPECT_EQ(params.at(INDEX_NUM_ROWS_KEY), kRows);
    }

    template <typename Input>
    static void
    ExpectDeclaredField(const IArtifactBuilder<Input>& builder) {
        const std::vector<FieldId> expected{FieldId(kFieldId)};
        EXPECT_EQ(builder.InputSpec().side_inputs, expected);
        EXPECT_EQ(builder.InputSpec().side_inputs, expected);
    }

    static void
    ExpectReaderResults(const IIndexReaderBase& base,
                        const std::string& index_type) {
        EXPECT_EQ(base.CoordDomain(), Domain::Row);
        EXPECT_EQ(base.ValueType(), DataType::VECTOR_FLOAT);
        EXPECT_EQ(base.Count(), kRows);
        const auto* vectors = dynamic_cast<const IVectorReader*>(&base);
        ASSERT_NE(vectors, nullptr);
        EXPECT_EQ(vectors->KnowhereIndexType(), index_type);
        EXPECT_EQ(vectors->Metric(), "L2");
        EXPECT_EQ(vectors->Dim(), kDim);

        VectorSearchParams params{
            .search_params_ = Config::object(),
            .metric_type_ = "L2",
            .topk_ = 3,
        };
        if (index_type == knowhere::IndexEnum::INDEX_DISKANN) {
            params.search_params_[DISK_ANN_QUERY_LIST] = 64;
        } else {
            params.search_params_[knowhere::indexparam::EF] = 64;
        }
        for (int64_t parity : {0, 1}) {
            SCOPED_TRACE(::testing::Message() << "category parity=" << parity);
            // Select one scalar category, then exclude all but three of its
            // rows. The query's own row is excluded. Distances are 30*d*d.
            const std::vector<int64_t> expected{
                parity + 2, parity + 6, parity + 10};
            std::vector<uint8_t> excluded((kRows + 7) / 8, 0xff);
            for (const auto row : expected) {
                excluded[row / 8] &=
                    static_cast<uint8_t>(~(1U << (row % 8)));
            }
            const std::array<float, kDim> query{
                static_cast<float>(parity),
                static_cast<float>(2 * parity),
                static_cast<float>(3 * parity),
                static_cast<float>(4 * parity)};
            SearchResult result;
            vectors->Search(GenDataset(1, kDim, query.data()),
                            params,
                            BitsetView(excluded.data(), kRows),
                            nullptr,
                            result);
            EXPECT_EQ(result.total_nq_, 1);
            EXPECT_EQ(result.unity_topK_, 3);
            EXPECT_EQ(result.seg_offsets_, expected);
            ASSERT_EQ(result.distances_.size(), 3);
            EXPECT_FLOAT_EQ(result.distances_[0], 120.0F);
            EXPECT_FLOAT_EQ(result.distances_[1], 1080.0F);
            EXPECT_FLOAT_EQ(result.distances_[2], 3000.0F);
        }

        ASSERT_TRUE(vectors->HasRawData());
        // Unordered, repeated IDs cross both categories and include endpoints.
        const std::array<int64_t, 5> ids{kRows - 1, 0, 257, 2, kRows - 1};
        const auto bytes =
            vectors->GetVector(GenIdsDataset(ids.size(), ids.data()));
        ASSERT_EQ(bytes.size(), ids.size() * kDim * sizeof(float));
        std::vector<float> decoded(ids.size() * kDim);
        std::memcpy(decoded.data(), bytes.data(), bytes.size());
        for (size_t i = 0; i < ids.size(); ++i) {
            for (int64_t col = 0; col < kDim; ++col) {
                EXPECT_FLOAT_EQ(decoded[i * kDim + col],
                                static_cast<float>(ids[i] * (col + 1)));
            }
        }
    }

    static bool
    DiskBackendSupportsScalarInput(
        const std::shared_ptr<storage::LocalDirectory>& directory) {
        // Probe the real engine independently of the InputSpec under test.
        // A regression dropping the declaration must fail, not turn into skip.
        auto manager =
            std::make_shared<VectorDiskBuildFileManager>(directory);
        auto pack = knowhere::Pack(
            std::static_pointer_cast<milvus::FileManager>(manager));
        const KnowhereEngine engine(
            DataType::VECTOR_FLOAT,
            DataType::NONE,
            knowhere::IndexEnum::INDEX_DISKANN,
            "L2",
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            manager,
            pack);
        return engine.native_index.IsAdditionalScalarSupported(true);
    }

    static void
    WritePreparedFiles(const PhysicalInput& input,
                       const std::string& raw_path,
                       const std::string& scalar_path) {
        std::ofstream raw(raw_path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(raw.good());
        const auto rows = static_cast<uint32_t>(kRows);
        const auto dim = static_cast<uint32_t>(kDim);
        raw.write(reinterpret_cast<const char*>(&rows), sizeof(rows));
        raw.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
        raw.write(reinterpret_cast<const char*>(input.values.data()),
                  static_cast<std::streamsize>(input.values.size() *
                                               sizeof(float)));
        raw.close();
        ASSERT_FALSE(raw.fail());

        // Version-0 optional-field sidecar: uint8 version, uint32 field count,
        // int64 field ID, uint32 category count, then uint32 count + row IDs
        // per category. These are compact physical vector coordinates.
        std::ofstream scalar(scalar_path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(scalar.good());
        const uint8_t version = 0;
        const uint32_t field_count = 1;
        const int64_t field_id = kFieldId;
        const uint32_t category_count = input.categories.size();
        scalar.write(reinterpret_cast<const char*>(&version), sizeof(version));
        scalar.write(reinterpret_cast<const char*>(&field_count),
                     sizeof(field_count));
        scalar.write(reinterpret_cast<const char*>(&field_id), sizeof(field_id));
        scalar.write(reinterpret_cast<const char*>(&category_count),
                     sizeof(category_count));
        for (const auto& category : input.categories) {
            const auto count = static_cast<uint32_t>(category.size());
            scalar.write(reinterpret_cast<const char*>(&count), sizeof(count));
            scalar.write(reinterpret_cast<const char*>(category.data()),
                         static_cast<std::streamsize>(category.size() *
                                                      sizeof(uint32_t)));
        }
        scalar.close();
        ASSERT_FALSE(scalar.fail());
    }
};

}  // namespace milvus::index::test
