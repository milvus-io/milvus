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
#include <cstdint>
#include <memory>
#include <vector>

#include "common/QueryResult.h"

namespace milvus {
namespace {

TEST(QueryResultFilterTest, CopiesContiguousBackendWindow) {
    TargetBitmap source(32, false);
    source.set(8);
    source.set(17);
    BitsetView filter(source.view());
    filter.set_id_offset(8);
    filter.set_vector_count(10);
    filter.set_filter_count(2);

    SearchResult result;
    result.SetVectorIteratorRecreator(filter, {});
    const auto* copied = result.GetVectorIteratorBaseFilter();
    ASSERT_NE(copied, nullptr);
    ASSERT_EQ(copied->size(), filter.size());
    for (size_t i = 0; i < filter.size(); ++i) {
        EXPECT_EQ((*copied)[i], filter.test(i)) << i;
    }
    EXPECT_EQ(copied->count(), 2);
    source.reset();
    EXPECT_TRUE((*copied)[0]);
    EXPECT_TRUE((*copied)[9]);
    EXPECT_EQ(result.GetVectorIteratorBaseFilter(), copied);
}

TEST(QueryResultFilterTest, CopiesBackendBoundaryExclusions) {
    TargetBitmap source(17, false);
    BitsetView filter(source.view());
    filter.set_vector_count(23);
    filter.set_filter_count(6);

    SearchResult result;
    result.SetVectorIteratorRecreator(filter, {});
    const auto* copied = result.GetVectorIteratorBaseFilter();
    ASSERT_NE(copied, nullptr);
    ASSERT_EQ(copied->size(), 23);
    for (size_t i = 0; i < copied->size(); ++i) {
        EXPECT_EQ((*copied)[i], i >= 17) << i;
    }
    EXPECT_EQ(copied->count(), 6);
}

TEST(QueryResultFilterTest, LazyMappedCopyRetainsPinnedSourceLifetime) {
    auto source = std::make_shared<TargetBitmap>(17, false);
    source->set(8);
    const std::vector<int32_t> ids{8, 1, -1, 30};
    knowhere::IdArray mapping(ids.data(), ids.size());
    BitsetView filter(source->view());
    filter.set_out_ids(mapping, ids.size());
    filter.set_vector_count(ids.size());
    filter.set_filter_count(3);

    SearchResult result;
    result.vector_iterator_filter_owner_ = source;
    result.SetVectorIteratorRecreator(filter, {});
    EXPECT_FALSE(result.vector_iterator_base_filter_);
    source.reset();

    const auto* copied = result.GetVectorIteratorBaseFilter();
    ASSERT_NE(copied, nullptr);
    ASSERT_EQ(copied->size(), ids.size());
    EXPECT_TRUE((*copied)[0]);
    EXPECT_FALSE((*copied)[1]);
    EXPECT_TRUE((*copied)[2]);
    EXPECT_TRUE((*copied)[3]);
    EXPECT_EQ(copied->count(), 3);
    result.ClearVectorIteratorRecreator();
    EXPECT_FALSE(result.vector_iterator_base_filter_);
    EXPECT_FALSE(result.vector_iterator_filter_owner_);
    EXPECT_EQ(result.GetVectorIteratorBaseFilter(), nullptr);
}

}  // namespace
}  // namespace milvus
