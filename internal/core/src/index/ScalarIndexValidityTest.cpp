// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICEN_SE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRAN_TIES OR CON_DITION_S OF AN_Y KIN_D, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

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

TEST(ScalarIndexValidity, SharedSealedSnapshotsOutliveIndexAndIsolateWrites) {
    auto idx =
        std::make_unique<ValidityIndex>(130, std::vector<size_t>{1, 65, 129});
    std::vector<index::ScalarIndexValidity> leaves;
    for (int i = 0; i < 32; ++i) {
        leaves.emplace_back(
            exec::SegmentExpr::GetIndexValiditySnapshot(idx.get()));
        EXPECT_EQ(leaves.back().bitmap().get(), leaves.front().bitmap().get());
    }
    EXPECT_EQ(idx->owned_validity_calls, 0);
    idx.reset();

    TargetBitmap first;
    leaves[0].AppendTo(first, 63, 7);
    ASSERT_EQ(first.size(), 7);
    EXPECT_FALSE(first[2]);
    first.flip();
    TargetBitmap independent;
    leaves[1].AppendTo(independent, 63, 7);
    EXPECT_FALSE(independent[2]);
    EXPECT_TRUE(independent[0]);
    EXPECT_FALSE(leaves[2][129]);
    EXPECT_TRUE(leaves[2][128]);
    EXPECT_ANY_THROW(leaves[0].AppendTo(independent, 129, 2));
    EXPECT_ANY_THROW(leaves[0][-1]);
}

TEST(ScalarIndexValidity, AllValidSentinelMaterializesOnlyRequestedRange) {
    ValidityIndex idx(1000000, {});
    auto snapshot = exec::SegmentExpr::GetIndexValiditySnapshot(&idx);
    EXPECT_EQ(idx.owned_validity_calls, 0);
    EXPECT_EQ(snapshot.bitmap()->size(), 0);
    EXPECT_EQ(snapshot.size(), 1000000);
    EXPECT_TRUE(snapshot.all());
    TargetBitmap batch;
    snapshot.AppendTo(batch, 999993, 7);
    EXPECT_EQ(batch.size(), 7);
    EXPECT_EQ(batch.count(), 7);
    batch.reset();
    EXPECT_TRUE(snapshot[999999]);
    TargetBitmap empty;
    snapshot.AppendTo(empty, 1000000, 0);
    EXPECT_EQ(empty.size(), 0);

    // Appending an unaligned suffix preserves all existing prefix bits.
    TargetBitmap prefixed(5, false);
    prefixed.set(2);
    snapshot.AppendTo(prefixed, 123, 7);
    ASSERT_EQ(prefixed.size(), 12);
    for (size_t i = 0; i < prefixed.size(); ++i) {
        EXPECT_EQ(prefixed[i], i == 2 || i >= 5) << i;
    }
}

TEST(ScalarIndexValidity, GrowingUsesOwnedVirtualSnapshot) {
    ValidityIndex idx(130, {1, 65, 129}, true);
    EXPECT_FALSE(idx.GetImmutableValidity().has_value());
    auto first = exec::SegmentExpr::GetIndexValiditySnapshot(&idx);
    auto second = exec::SegmentExpr::GetIndexValiditySnapshot(&idx);
    EXPECT_EQ(idx.owned_validity_calls, 2);
    EXPECT_NE(first.bitmap().get(), second.bitmap().get());
    EXPECT_FALSE(first[65]);
    EXPECT_TRUE(first[66]);
}

}  // namespace
}  // namespace milvus::test
