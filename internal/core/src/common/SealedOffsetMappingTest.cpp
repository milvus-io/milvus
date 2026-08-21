// Copyright (C) 2019-2026 Zilliz. All rights reserved.
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

#include <vector>

#include "common/SealedOffsetMapping.h"

namespace milvus {

namespace {
std::vector<bool>
MakeValid(std::initializer_list<int> bits) {
    std::vector<bool> v;
    v.reserve(bits.size());
    for (int b : bits) {
        v.push_back(b != 0);
    }
    return v;
}

std::vector<uint8_t>
ToBoolBytes(const std::vector<bool>& valid) {
    std::vector<uint8_t> bytes(valid.size());
    for (size_t i = 0; i < valid.size(); ++i) {
        bytes[i] = valid[i] ? 1 : 0;
    }
    return bytes;
}
}  // namespace

// ---------- Build ----------

TEST(SealedOffsetMapping, BuildBasicVecMode) {
    SealedOffsetMapping mapping;
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 1, 0}));
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 5);

    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetTotalCount(), 5);
    EXPECT_EQ(mapping.GetValidCount(), 3);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(1), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(3), 2);
    EXPECT_EQ(mapping.GetPhysicalOffset(4), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(0), 0);
    EXPECT_EQ(mapping.GetLogicalOffset(1), 2);
    EXPECT_EQ(mapping.GetLogicalOffset(2), 3);
}

TEST(SealedOffsetMapping, BuildSparseUsesContiguousStorage) {
    SealedOffsetMapping mapping;
    std::vector<uint8_t> valid(100, 0);
    valid[5] = 1;
    valid[50] = 1;
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 100);

    EXPECT_EQ(mapping.GetTotalCount(), 100);
    EXPECT_EQ(mapping.GetValidCount(), 2);
    EXPECT_EQ(mapping.GetPhysicalOffset(5), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(50), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(0), 5);
    EXPECT_EQ(mapping.GetLogicalOffset(1), 50);
}

TEST(SealedOffsetMapping, BuildAllValid) {
    SealedOffsetMapping mapping;
    std::vector<uint8_t> valid(4, 1);
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
    EXPECT_EQ(mapping.GetValidCount(), 4);
    for (int64_t i = 0; i < 4; ++i) {
        EXPECT_EQ(mapping.GetPhysicalOffset(i), i);
        EXPECT_EQ(mapping.GetLogicalOffset(i), i);
    }
}

TEST(SealedOffsetMapping, BuildAllNull) {
    SealedOffsetMapping mapping;
    std::vector<uint8_t> valid(4, 0);
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetValidCount(), 0);
    EXPECT_EQ(mapping.GetTotalCount(), 4);
    for (int64_t i = 0; i < 4; ++i) {
        EXPECT_EQ(mapping.GetPhysicalOffset(i), -1);
    }
}

TEST(SealedOffsetMapping, BuildNoopOnNullOrZero) {
    SealedOffsetMapping mapping;
    mapping.Build(nullptr, 100);
    EXPECT_FALSE(mapping.IsEnabled());
    std::vector<uint8_t> valid(1, 1);
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 0);
    EXPECT_FALSE(mapping.IsEnabled());
}

TEST(SealedOffsetMapping, BuildTwiceResetsState) {
    SealedOffsetMapping mapping;
    auto v1 = ToBoolBytes(MakeValid({1, 1, 0, 0, 1}));
    mapping.Build(reinterpret_cast<const bool*>(v1.data()), 5);
    EXPECT_EQ(mapping.GetValidCount(), 3);
    EXPECT_EQ(mapping.GetTotalCount(), 5);

    auto v2 = ToBoolBytes(MakeValid({1, 0, 0}));
    mapping.Build(reinterpret_cast<const bool*>(v2.data()), 3);
    EXPECT_EQ(mapping.GetValidCount(), 1);
    EXPECT_EQ(mapping.GetTotalCount(), 3);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(1), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), -1);
}

// A degenerate rebuild must not silently retain the previous mapping: the
// header contract says null/empty input leaves the mapping disabled.
TEST(SealedOffsetMapping, BuildWithEmptyInputResetsPreviousBuild) {
    SealedOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 3);
    ASSERT_TRUE(mapping.IsEnabled());
    ASSERT_EQ(mapping.GetValidCount(), 2);

    mapping.Build(nullptr, 100);
    EXPECT_FALSE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetTotalCount(), 0);
    EXPECT_EQ(mapping.GetValidCount(), 0);
    // Disabled again: conversions fall back to identity.
    EXPECT_EQ(mapping.GetPhysicalOffset(1), 1);
    EXPECT_EQ(mapping.ValidCountBelow(42), 42);

    mapping.Build(reinterpret_cast<const bool*>(v.data()), 3);
    ASSERT_TRUE(mapping.IsEnabled());
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 0);
    EXPECT_FALSE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetTotalCount(), 0);
}

// ---------- IsValid ----------

TEST(SealedOffsetMapping, IsValidMatchesPhysicalOffsetSign) {
    SealedOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 0}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 4);
    EXPECT_TRUE(mapping.IsValid(0));
    EXPECT_FALSE(mapping.IsValid(1));
    EXPECT_TRUE(mapping.IsValid(2));
    EXPECT_FALSE(mapping.IsValid(3));
}

// ---------- Out-of-bounds queries ----------

TEST(SealedOffsetMapping, OutOfBoundsReturnsMinusOne) {
    SealedOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 3);
    EXPECT_EQ(mapping.GetPhysicalOffset(99), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(99), -1);
}

// ---------- ValidCountBelow ----------

// logical: 0(v) 1(x) 2(v) 3(v) 4(x) 5(v)
// physical:  0        1    2         3
TEST(SealedOffsetMapping, ValidCountBelowConvertsLogicalBound) {
    SealedOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 1, 0, 1}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 6);
    ASSERT_EQ(mapping.GetValidCount(), 4);

    EXPECT_EQ(mapping.ValidCountBelow(0), 0);
    EXPECT_EQ(mapping.ValidCountBelow(3), 2);
    EXPECT_EQ(mapping.ValidCountBelow(6), 4);
    EXPECT_EQ(mapping.ValidCountBelow(100), 4);
}

// Sparse input still uses the same contiguous physical->logical storage.
TEST(SealedOffsetMapping, ValidCountBelowOnSparseMapping) {
    SealedOffsetMapping mapping;
    std::vector<uint8_t> valid(100, 0);
    valid[5] = 1;
    valid[50] = 1;
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 100);
    ASSERT_EQ(mapping.GetValidCount(), 2);

    EXPECT_EQ(mapping.ValidCountBelow(0), 0);
    EXPECT_EQ(mapping.ValidCountBelow(5), 0);
    EXPECT_EQ(mapping.ValidCountBelow(6), 1);
    EXPECT_EQ(mapping.ValidCountBelow(50), 1);
    EXPECT_EQ(mapping.ValidCountBelow(51), 2);
    EXPECT_EQ(mapping.ValidCountBelow(100), 2);
}

// A never-built mapping is disabled, so the bound passes through unchanged.
TEST(SealedOffsetMapping, ValidCountBelowIsIdentityBeforeBuild) {
    SealedOffsetMapping mapping;
    ASSERT_FALSE(mapping.IsEnabled());
    EXPECT_EQ(mapping.ValidCountBelow(42), 42);
    EXPECT_EQ(mapping.ValidCountBelow(-5), 0);
}

}  // namespace milvus
