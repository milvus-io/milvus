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

#include "exec/expression/Expr.h"
#include "index/InvertedIndexTantivy.h"

namespace milvus::test {
namespace {

// Exercise the production Tantivy validity methods independently of disk I/O.
// Count and finalized null offsets model a fixed sealed row space.
class ValidityIndex : public index::InvertedIndexTantivy<int64_t> {
 public:
    ValidityIndex(int64_t rows, std::vector<size_t> nulls, bool growing = false)
        : rows_(rows) {
        null_offset_ = std::move(nulls);
        set_is_growing(growing);
        if (!growing) {
            FinalizeSealed();
        }
    }

    int64_t
    Count() override {
        return rows_;
    }

    void
    ComputeByteSize() override {
    }

    TargetBitmap
    IsNotNull() override {
        ++owned_validity_calls;
        return index::InvertedIndexTantivy<int64_t>::IsNotNull();
    }

    int owned_validity_calls = 0;

 private:
    int64_t rows_;
};

TEST(IndexValidityView, PinKeepsBorrowedBufferAliveAndOutputsIndependent) {
    auto idx =
        std::make_shared<ValidityIndex>(130, std::vector<size_t>{1, 65, 129});
    std::weak_ptr<ValidityIndex> weak = idx;
    milvus::cachinglayer::PinWrapper<const index::IndexBase*> pin(idx,
                                                                  idx.get());
    auto* selected = idx.get();
    idx.reset();
    ASSERT_FALSE(weak.expired());
    std::vector<BitsetView> leaves;
    for (int i = 0; i < 32; ++i) {
        auto view = selected->GetValidityView();
        ASSERT_TRUE(view.has_value());
        leaves.emplace_back(*view);
        EXPECT_EQ(leaves.back().data(), leaves.front().data());
    }
    EXPECT_EQ(selected->owned_validity_calls, 0);
    TargetBitmap first;
    exec::AppendValidityView(first, leaves[0], selected->Count(), 63, 7);
    ASSERT_EQ(first.size(), 7);
    EXPECT_FALSE(first[2]);
    first.flip();
    TargetBitmap independent;
    exec::AppendValidityView(independent, leaves[1], selected->Count(), 63, 7);
    EXPECT_FALSE(independent[2]);
    EXPECT_TRUE(independent[0]);
    EXPECT_FALSE(leaves[2].test(129));
    EXPECT_TRUE(leaves[2].test(128));
    EXPECT_ANY_THROW(exec::AppendValidityView(
        independent, leaves[0], selected->Count(), 129, 2));
    // Views do not extend ownership beyond the pin; never read them after this.
    pin = {};
    EXPECT_TRUE(weak.expired());
}

TEST(IndexValidityView, AllValidSentinelMaterializesOnlyRequestedRange) {
    ValidityIndex idx(1000000, {});
    auto view = idx.GetValidityView();
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->size(), 0);
    EXPECT_EQ(idx.owned_validity_calls, 0);
    TargetBitmap batch;
    exec::AppendValidityView(batch, *view, idx.Count(), 999993, 7);
    EXPECT_EQ(batch.size(), 7);
    EXPECT_EQ(batch.count(), 7);
    TargetBitmap empty;
    exec::AppendValidityView(empty, *view, idx.Count(), 1000000, 0);
    EXPECT_EQ(empty.size(), 0);
    TargetBitmap prefixed(5, false);
    prefixed.set(2);
    exec::AppendValidityView(prefixed, *view, idx.Count(), 123, 7);
    ASSERT_EQ(prefixed.size(), 12);
    for (size_t i = 0; i < prefixed.size(); ++i) {
        EXPECT_EQ(prefixed[i], i == 2 || i >= 5) << i;
    }
}

TEST(IndexValidityView, AllNullDenseBitmapIsNotAllValidSentinel) {
    ValidityIndex idx(3, {0, 1, 2});
    auto view = idx.GetValidityView();
    ASSERT_TRUE(view.has_value());
    ASSERT_EQ(view->size(), 3);
    TargetBitmap batch;
    exec::AppendValidityView(batch, *view, idx.Count(), 0, 3);
    EXPECT_EQ(batch.size(), 3);
    EXPECT_EQ(batch.count(), 0);
}

TEST(IndexValidityView, GrowingKeepsOwnedFallbackAndItsActualSize) {
    ValidityIndex idx(130, {1, 65, 129}, true);
    EXPECT_FALSE(idx.GetValidityView().has_value());
    std::shared_ptr<TargetBitmap> first_owner, second_owner;
    int64_t first_count = 0, second_count = 0;
    auto first =
        exec::SegmentExpr::GetIndexValidityView(&idx, first_owner, first_count);
    auto second = exec::SegmentExpr::GetIndexValidityView(
        &idx, second_owner, second_count);
    EXPECT_EQ(idx.owned_validity_calls, 2);
    ASSERT_NE(first_owner, nullptr);
    EXPECT_EQ(first_count, first_owner->size());
    EXPECT_EQ(second_count, second_owner->size());
    EXPECT_NE(first.data(), second.data());
    EXPECT_FALSE(first.test(65));
    EXPECT_TRUE(first.test(66));
}

}  // namespace
}  // namespace milvus::test
