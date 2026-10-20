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
#include <numeric>

#include "cachinglayer/CacheSlot.h"
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
    auto owner =
        std::make_shared<ValidityIndex>(130, std::vector<size_t>{1, 65, 129});
    std::weak_ptr<ValidityIndex> weak = owner;
    milvus::cachinglayer::PinWrapper<const index::IndexBase*> pin(owner,
                                                                  owner.get());
    auto* idx = owner.get();
    owner.reset();
    ASSERT_FALSE(weak.expired());
    std::vector<BitsetView> leaves;
    std::shared_ptr<TargetBitmap> fallback;
    int64_t rows = 0;
    for (int i = 0; i < 32; ++i) {
        leaves.emplace_back(
            exec::SegmentExpr::GetIndexValidityView(idx, fallback, rows));
        EXPECT_EQ(leaves.back().data(), leaves.front().data());
    }
    EXPECT_EQ(rows, 130);
    EXPECT_EQ(idx->owned_validity_calls, 0);
    EXPECT_EQ(fallback, nullptr);

    TargetBitmap first;
    exec::AppendIndexValidity(first, leaves[0], 63, 7, rows);
    ASSERT_EQ(first.size(), 7);
    EXPECT_FALSE(first[2]);
    first.flip();
    TargetBitmap independent;
    exec::AppendIndexValidity(independent, leaves[1], 63, 7, rows);
    EXPECT_FALSE(independent[2]);
    EXPECT_TRUE(independent[0]);
    EXPECT_FALSE(leaves[2].test(129));
    EXPECT_TRUE(leaves[2].test(128));
    EXPECT_ANY_THROW(
        exec::AppendIndexValidity(independent, leaves[0], 129, 2, rows));

    // Neither source nor destination needs byte alignment.
    TargetBitmap prefixed(5, true);
    exec::AppendIndexValidity(prefixed, leaves[0], 63, 67, rows);
    ASSERT_EQ(prefixed.size(), 72);
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(prefixed[i]);
    }
    for (size_t i = 0; i < 67; ++i) {
        EXPECT_EQ(prefixed[5 + i], leaves[0].test(63 + i));
    }
    // A view does not keep storage alive. No view is read after the pin drops.
    pin = {};
    EXPECT_TRUE(weak.expired());
}

TEST(IndexValidityView, AllValidSentinelMaterializesOnlyRequestedRange) {
    ValidityIndex idx(1000000, {});
    std::shared_ptr<TargetBitmap> fallback;
    int64_t rows = 0;
    auto view = exec::SegmentExpr::GetIndexValidityView(&idx, fallback, rows);
    EXPECT_EQ(idx.owned_validity_calls, 0);
    EXPECT_EQ(view.size(), 0);
    EXPECT_EQ(rows, 1000000);
    EXPECT_EQ(fallback, nullptr);
    TargetBitmap batch;
    exec::AppendIndexValidity(batch, view, 999993, 7, rows);
    EXPECT_EQ(batch.size(), 7);
    EXPECT_EQ(batch.count(), 7);
    TargetBitmap empty;
    exec::AppendIndexValidity(empty, view, 1000000, 0, rows);
    EXPECT_EQ(empty.size(), 0);
    EXPECT_ANY_THROW(exec::AppendIndexValidity(empty, view, rows, 1, rows));

    TargetBitmap prefixed(5, false);
    prefixed.set(2);
    exec::AppendIndexValidity(prefixed, view, 123, 7, rows);
    ASSERT_EQ(prefixed.size(), 12);
    for (size_t i = 0; i < prefixed.size(); ++i) {
        EXPECT_EQ(prefixed[i], i == 2 || i >= 5) << i;
    }
}

TEST(IndexValidityView, DenseAllInvalidIsNotTheAllValidSentinel) {
    std::vector<size_t> nulls(130);
    std::iota(nulls.begin(), nulls.end(), 0);
    ValidityIndex idx(130, std::move(nulls));
    auto view = idx.GetValidityView();
    ASSERT_TRUE(view.has_value());
    ASSERT_EQ(view->size(), 130);
    TargetBitmap output(5, true);
    exec::AppendIndexValidity(output, *view, 63, 67, idx.Count());
    EXPECT_EQ(output.size(), 72);
    EXPECT_EQ(output.count(), 5);
    for (size_t i = 5; i < output.size(); ++i) {
        EXPECT_FALSE(output[i]);
    }
}

TEST(IndexValidityView, GrowingKeepsOwnedVirtualFallback) {
    ValidityIndex idx(130, {1, 65, 129}, true);
    EXPECT_FALSE(idx.GetValidityView().has_value());
    std::shared_ptr<TargetBitmap> first_owner, second_owner;
    int64_t first_rows = 0, second_rows = 0;
    auto first =
        exec::SegmentExpr::GetIndexValidityView(&idx, first_owner, first_rows);
    auto second = exec::SegmentExpr::GetIndexValidityView(
        &idx, second_owner, second_rows);
    EXPECT_EQ(idx.owned_validity_calls, 2);
    ASSERT_NE(first_owner, nullptr);
    ASSERT_NE(second_owner, nullptr);
    EXPECT_NE(first.data(), second.data());
    EXPECT_EQ(first_rows, 130);
    EXPECT_EQ(second_rows, 130);
    EXPECT_FALSE(first.test(65));
    EXPECT_TRUE(first.test(66));
}

}  // namespace
}  // namespace milvus::test
