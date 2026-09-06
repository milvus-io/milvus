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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/QueryResult.h"
#include "gtest/gtest.h"

namespace milvus {
namespace {

class FixedKnowhereIterator final : public knowhere::IndexNode::iterator {
 public:
    explicit FixedKnowhereIterator(
        std::vector<std::pair<int64_t, float>> results)
        : results_(std::move(results)) {
    }

    knowhere::expected<std::pair<int64_t, float>>
    Next() noexcept override {
        if (!HasNext().value()) {
            return knowhere::expected<std::pair<int64_t, float>>::Err(
                knowhere::Status::knowhere_inner_error,
                "fixed iterator has no next result");
        }
        return results_[next_++];
    }

    knowhere::expected<bool>
    HasNext() noexcept override {
        return next_ < results_.size();
    }

 private:
    std::vector<std::pair<int64_t, float>> results_;
    size_t next_ = 0;
};

enum class IteratorErrorPoint { HasNext, Next };

class ErrorKnowhereIterator final : public knowhere::IndexNode::iterator {
 public:
    explicit ErrorKnowhereIterator(IteratorErrorPoint error_point)
        : error_point_(error_point) {
    }

    knowhere::expected<std::pair<int64_t, float>>
    Next() noexcept override {
        if (error_point_ == IteratorErrorPoint::Next) {
            return knowhere::expected<std::pair<int64_t, float>>::Err(
                knowhere::Status::knowhere_inner_error,
                "injected Next failure");
        }
        return std::pair<int64_t, float>{1, 0.1F};
    }

    knowhere::expected<bool>
    HasNext() noexcept override {
        if (error_point_ == IteratorErrorPoint::HasNext) {
            return knowhere::expected<bool>::Err(
                knowhere::Status::knowhere_inner_error,
                "injected HasNext failure");
        }
        return true;
    }

 private:
    IteratorErrorPoint error_point_;
};

knowhere::IndexNode::IteratorPtr
MakeIterator(std::vector<std::pair<int64_t, float>> results) {
    return std::make_shared<FixedKnowhereIterator>(std::move(results));
}

std::vector<std::pair<int64_t, float>>
Collect(const std::shared_ptr<VectorIterator>& iterator) {
    std::vector<std::pair<int64_t, float>> results;
    while (iterator->HasNext()) {
        auto next = iterator->Next();
        if (!next.has_value()) {
            ADD_FAILURE() << "iterator returned no value after HasNext";
            break;
        }
        results.emplace_back(next.value());
    }
    return results;
}

TEST(QueryResult, SingleChunkUsesDirectAdapter) {
    SearchResult result;
    std::vector<knowhere::IndexNode::IteratorPtr> iterators{
        MakeIterator({{1, 0.1F}, {2, 0.2F}}), MakeIterator({})};

    result.AssembleChunkVectorIterators(2, 1, iterators);

    ASSERT_TRUE(result.vector_iterators_.has_value());
    ASSERT_EQ(result.vector_iterators_->size(), 2);
    EXPECT_NE(std::dynamic_pointer_cast<KnowhereVectorIteratorAdapter>(
                  result.vector_iterators_->at(0)),
              nullptr);
    EXPECT_EQ(Collect(result.vector_iterators_->at(0)),
              (std::vector<std::pair<int64_t, float>>{{1, 0.1F}, {2, 0.2F}}));
    EXPECT_FALSE(result.vector_iterators_->at(1)->HasNext());
    EXPECT_FALSE(result.vector_iterators_->at(1)->Next().has_value());
}

TEST(QueryResult, SingleChunkHandlesNullIterator) {
    SearchResult result;
    std::vector<knowhere::IndexNode::IteratorPtr> iterators{nullptr};

    result.AssembleChunkVectorIterators(1, 1, iterators);

    ASSERT_TRUE(result.vector_iterators_.has_value());
    ASSERT_EQ(result.vector_iterators_->size(), 1);
    EXPECT_FALSE(result.vector_iterators_->front()->HasNext());
    EXPECT_FALSE(result.vector_iterators_->front()->Next().has_value());
}

TEST(QueryResult, SingleChunkPropagatesHasNextError) {
    try {
        auto iterator = std::make_shared<KnowhereVectorIteratorAdapter>(
            std::make_shared<ErrorKnowhereIterator>(
                IteratorErrorPoint::HasNext));
        (void)iterator;
        FAIL() << "expected HasNext failure";
    } catch (const SegcoreError& error) {
        EXPECT_NE(
            std::string(error.what()).find("knowhere iterator HasNext failed"),
            std::string::npos);
    }
}

TEST(QueryResult, SingleChunkPropagatesNextError) {
    try {
        auto iterator = std::make_shared<KnowhereVectorIteratorAdapter>(
            std::make_shared<ErrorKnowhereIterator>(IteratorErrorPoint::Next));
        (void)iterator;
        FAIL() << "expected Next failure";
    } catch (const SegcoreError& error) {
        EXPECT_NE(
            std::string(error.what()).find("knowhere iterator Next failed"),
            std::string::npos);
    }
}

TEST(QueryResult, MultiChunkMergesL2WithLeadingEmptyIterator) {
    SearchResult result;
    std::vector<knowhere::IndexNode::IteratorPtr> iterators{
        MakeIterator({}),
        MakeIterator({{10, 0.1F}, {11, 0.4F}}),
        MakeIterator({{20, 0.1F}, {21, 0.3F}})};

    result.AssembleChunkVectorIterators(1, 3, iterators);

    ASSERT_TRUE(result.vector_iterators_.has_value());
    ASSERT_EQ(result.vector_iterators_->size(), 1);
    EXPECT_NE(std::dynamic_pointer_cast<ChunkMergeIterator>(
                  result.vector_iterators_->front()),
              nullptr);
    EXPECT_EQ(Collect(result.vector_iterators_->front()),
              (std::vector<std::pair<int64_t, float>>{
                  {20, 0.1F}, {10, 0.1F}, {21, 0.3F}, {11, 0.4F}}));
}

TEST(QueryResult, MultiChunkMergesLargerIsCloser) {
    SearchResult result;
    std::vector<knowhere::IndexNode::IteratorPtr> iterators{
        MakeIterator({{10, 0.9F}, {11, 0.7F}}),
        MakeIterator({{20, 0.8F}, {21, 0.6F}})};

    result.AssembleChunkVectorIterators(1, 2, iterators, true);

    ASSERT_TRUE(result.vector_iterators_.has_value());
    ASSERT_EQ(result.vector_iterators_->size(), 1);
    EXPECT_EQ(Collect(result.vector_iterators_->front()),
              (std::vector<std::pair<int64_t, float>>{
                  {10, 0.9F}, {20, 0.8F}, {11, 0.7F}, {21, 0.6F}}));
}

}  // namespace
}  // namespace milvus
