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
#include <roaring/roaring.hh>

#include <map>
#include <string>

#include "common/Types.h"
#include "index/IndexFactory.h"
#include "index/InvertedIndexUtil.h"
#include "index/Meta.h"

namespace milvus::index {

TEST(IndexFactoryRawDataTest, JsonPathIndexCannotReplaceWholeJsonField) {
    std::map<std::string, std::string> index_params{
        {INDEX_TYPE, ASCENDING_SORT}};
    auto& factory = IndexFactory::GetInstance();

    auto json_request = factory.ScalarIndexLoadResource(
        DataType::JSON, 0, 1024, index_params, false, 1000);
    EXPECT_FALSE(json_request.has_raw_data);

    auto varchar_request = factory.ScalarIndexLoadResource(
        DataType::VARCHAR, 0, 1024, index_params, false, 1000);
    EXPECT_TRUE(varchar_request.has_raw_data);
}

TEST(IndexFactoryRawDataTest, LoadResourceIncludesResidentRoaringRowMasks) {
    constexpr int64_t kRowCount = 65536;
    constexpr uint64_t kIndexSize = 1024;
    constexpr uint64_t kOneMaskBudget = kRowCount / 4;

    std::map<std::string, std::string> plain_params{
        {INDEX_TYPE, INVERTED_INDEX_TYPE},
        {SCALAR_INDEX_ENGINE_VERSION, "3"},
    };
    auto plain = IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::INT64, 0, kIndexSize, plain_params, true, kRowCount);
    EXPECT_EQ(plain.final_memory_cost, kOneMaskBudget);

    std::map<std::string, std::string> json_params{
        {INDEX_TYPE, INVERTED_INDEX_TYPE},
        {SCALAR_INDEX_ENGINE_VERSION, "3"},
        {JSON_PATH, "/a"},
        {JSON_CAST_TYPE, "DOUBLE"},
    };
    auto json = IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::JSON, 0, kIndexSize, json_params, true, kRowCount);
    EXPECT_EQ(json.final_memory_cost, 2 * kOneMaskBudget + kRowCount / 8);
}

TEST(IndexFactoryRawDataTest,
     LoadResourceSkipsRowMaskForNonNullableScalarField) {
    constexpr int64_t kRowCount = 65536;
    constexpr uint64_t kIndexSize = 1024;
    constexpr uint64_t kOneMaskBudget = kRowCount / 4;

    std::map<std::string, std::string> params{
        {INDEX_TYPE, INVERTED_INDEX_TYPE},
        {SCALAR_INDEX_ENGINE_VERSION, "3"},
    };
    auto non_nullable = IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::INT64, 0, kIndexSize, params, true, kRowCount, false);
    auto nullable = IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::INT64, 0, kIndexSize, params, true, kRowCount, true);

    EXPECT_EQ(non_nullable.final_memory_cost, 0);
    EXPECT_EQ(nullable.final_memory_cost, kOneMaskBudget);
}

TEST(IndexFactoryRawDataTest, LoadResourceCoversSparseRoaringRowMask) {
    constexpr int64_t kRowCount = 8192;

    roaring::Roaring sparse_offsets;
    for (uint32_t row = 0; row < kRowCount; row += 2) {
        sparse_offsets.add(row);
    }
    sparse_offsets.runOptimize();
    sparse_offsets.shrinkToFit();

    std::map<std::string, std::string> params{
        {INDEX_TYPE, INVERTED_INDEX_TYPE},
        {SCALAR_INDEX_ENGINE_VERSION, "3"},
    };
    auto request = IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::INT64,
        0,
        0,
        params,
        true,
        kRowCount,
        /*field_nullable=*/true);
    const auto actual_mask_bytes = RoaringMemoryBytes(sparse_offsets);

    EXPECT_GE(request.final_memory_cost, actual_mask_bytes);
    EXPECT_GE(request.max_memory_cost, actual_mask_bytes);
}

}  // namespace milvus::index
