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

#include <map>
#include <string>

#include "common/Types.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"

namespace milvus::index {

TEST(IndexFactoryRawDataTest, JsonPathIndexCannotReplaceWholeJsonField) {
    std::map<std::string, std::string> index_params{
        {INDEX_TYPE, ASCENDING_SORT}};
    auto& factory = IndexFactory::GetInstance();

    auto json_request = factory.ScalarIndexLoadResource(
        DataType::JSON, 0, 1024, index_params, false, 0);
    EXPECT_FALSE(json_request.has_raw_data);

    auto varchar_request = factory.ScalarIndexLoadResource(
        DataType::VARCHAR, 0, 1024, index_params, false, 0);
    EXPECT_TRUE(varchar_request.has_raw_data);
}

TEST(IndexFactoryResourceTest, TantivyValidityBitmapIsWordAligned) {
    constexpr int64_t kNumRows = 65;
    constexpr uint64_t kIndexSize = 1024;
    auto& factory = IndexFactory::GetInstance();
    auto estimate = [&](const std::string& index_type, bool mmap_enable) {
        std::map<std::string, std::string> index_params{
            {INDEX_TYPE, index_type}};
        return factory.ScalarIndexLoadResource(DataType::VARCHAR,
                                               0,
                                               kIndexSize,
                                               index_params,
                                               mmap_enable,
                                               kNumRows);
    };

    auto bitmap_bytes = TargetBitmap(kNumRows).size_in_bytes();
    for (bool mmap_enable : {false, true}) {
        auto rtree = estimate(RTREE_INDEX_TYPE, mmap_enable);
        EXPECT_EQ(rtree.final_memory_cost, 0);
        for (const auto& index_type : {INVERTED_INDEX_TYPE, NGRAM_INDEX_TYPE}) {
            auto request = estimate(index_type, mmap_enable);
            EXPECT_EQ(request.final_memory_cost,
                      mmap_enable ? bitmap_bytes : kIndexSize + bitmap_bytes);
            EXPECT_EQ(request.final_disk_cost, mmap_enable ? kIndexSize : 0);
            EXPECT_EQ(request.max_memory_cost, kIndexSize + bitmap_bytes);
            EXPECT_EQ(request.max_disk_cost, kIndexSize);
        }
    }
}

}  // namespace milvus::index
