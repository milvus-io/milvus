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

#include "common/OffsetMapping.h"

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

// ---------- Default (disabled) state ----------

TEST(OffsetMapping, DefaultIsDisabledAndPassThrough) {
    OffsetMapping mapping;
    EXPECT_FALSE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetValidCount(), 0);
    EXPECT_EQ(mapping.GetTotalCount(), 0);
    // When disabled, offset queries must pass through unchanged.
    EXPECT_EQ(mapping.GetPhysicalOffset(42), 42);
    EXPECT_EQ(mapping.GetLogicalOffset(42), 42);
}

// ---------- Build (eager) ----------

TEST(OffsetMapping, BuildBasicVecMode) {
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

TEST(OffsetMapping, BuildMapModeOnSparse) {
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

TEST(OffsetMapping, BuildAllValid) {
    SealedOffsetMapping mapping;
    std::vector<uint8_t> valid(4, 1);
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
    EXPECT_EQ(mapping.GetValidCount(), 4);
    for (int64_t i = 0; i < 4; ++i) {
        EXPECT_EQ(mapping.GetPhysicalOffset(i), i);
        EXPECT_EQ(mapping.GetLogicalOffset(i), i);
    }
}

TEST(OffsetMapping, BuildAllNull) {
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

TEST(OffsetMapping, BuildNoopOnNullOrZero) {
    SealedOffsetMapping mapping;
    mapping.Build(nullptr, 100);
    EXPECT_FALSE(mapping.IsEnabled());
    std::vector<uint8_t> valid(1, 1);
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 0);
    EXPECT_FALSE(mapping.IsEnabled());
}

TEST(OffsetMapping, BuildTwiceResetsState) {
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

// ---------- Append ----------

TEST(OffsetMapping, AppendBasic) {
    GrowingOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 1}));
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 4, 0, 0);

    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetValidCount(), 3);
    EXPECT_EQ(mapping.GetTotalCount(), 4);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(3), 2);
}

TEST(OffsetMapping, AppendMultipleBatches) {
    GrowingOffsetMapping mapping;
    auto b1 = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Append(reinterpret_cast<const bool*>(b1.data()), 3, 0, 0);
    EXPECT_EQ(mapping.GetValidCount(), 2);
    EXPECT_EQ(mapping.GetTotalCount(), 3);

    auto b2 = ToBoolBytes(MakeValid({0, 1, 1}));
    mapping.Append(reinterpret_cast<const bool*>(b2.data()),
                   3,
                   mapping.GetTotalCount(),
                   mapping.GetValidCount());
    EXPECT_EQ(mapping.GetValidCount(), 4);
    EXPECT_EQ(mapping.GetTotalCount(), 6);

    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(4), 2);
    EXPECT_EQ(mapping.GetPhysicalOffset(5), 3);
    EXPECT_EQ(mapping.GetLogicalOffset(3), 5);
}

TEST(OffsetMapping, AppendNoopOnNullOrZero) {
    GrowingOffsetMapping mapping;
    mapping.Append(nullptr, 3, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
    std::vector<uint8_t> v(1, 1);
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 0, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
}

// ---------- IsValid ----------

TEST(OffsetMapping, IsValidMatchesPhysicalOffsetSign) {
    SealedOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 0}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 4);
    EXPECT_TRUE(mapping.IsValid(0));
    EXPECT_FALSE(mapping.IsValid(1));
    EXPECT_TRUE(mapping.IsValid(2));
    EXPECT_FALSE(mapping.IsValid(3));
}

// ---------- Out-of-bounds queries ----------

TEST(OffsetMapping, OutOfBoundsReturnsMinusOne) {
    SealedOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 3);
    EXPECT_EQ(mapping.GetPhysicalOffset(99), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(99), -1);
}

}  // namespace milvus
