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

#include "common/QueryResult.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace milvus {
namespace {

class FixedKnowhereIterator : public knowhere::IndexNode::iterator {
 public:
    explicit FixedKnowhereIterator(
        std::vector<std::pair<int64_t, float>> results)
        : results_(std::move(results)) {
    }

    std::pair<int64_t, float>
    Next() override {
        return results_.at(position_++);
    }

    bool
    HasNext() override {
        return position_ < results_.size();
    }

 private:
    std::vector<std::pair<int64_t, float>> results_;
    size_t position_{0};
};

TEST(QueryResultTest, ChunkMergeIteratorKeepsIndexAfterEmptyChunk) {
    std::vector<knowhere::IndexNode::IteratorPtr> chunk_iterators{
        std::make_shared<FixedKnowhereIterator>(
            std::vector<std::pair<int64_t, float>>{}),
        std::make_shared<FixedKnowhereIterator>(
            std::vector<std::pair<int64_t, float>>{{10, 1.0F}, {11, 3.0F}}),
        std::make_shared<FixedKnowhereIterator>(
            std::vector<std::pair<int64_t, float>>{{20, 2.0F}, {21, 4.0F}}),
    };

    SearchResult search_result;
    search_result.AssembleChunkVectorIterators(
        1, 3, {}, chunk_iterators, nullptr);

    auto iterator = search_result.vector_iterators_->at(0);
    std::vector<std::pair<int64_t, float>> actual;
    while (iterator->HasNext()) {
        actual.push_back(iterator->Next().value());
    }

    const std::vector<std::pair<int64_t, float>> expected{
        {10, 1.0F}, {20, 2.0F}, {11, 3.0F}, {21, 4.0F}};
    EXPECT_EQ(actual, expected);
}

}  // namespace
}  // namespace milvus
