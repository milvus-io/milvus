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

#include <atomic>
#include <thread>
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

// ---------- Reserve ----------

TEST(OffsetMapping, ReserveReflectsTotalCounts) {
    OffsetMapping mapping;
    mapping.Reserve(10, 8, 2);
    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetTotalCount(), 10);
    EXPECT_EQ(mapping.GetValidCount(), 8);
    EXPECT_FALSE(mapping.IsChunkSet(0));
    EXPECT_FALSE(mapping.IsChunkSet(1));
}

TEST(OffsetMapping, ReserveAllValid) {
    OffsetMapping mapping;
    mapping.Reserve(5, 5, 1);
    EXPECT_EQ(mapping.GetValidCount(), 5);
    EXPECT_EQ(mapping.GetTotalCount(), 5);
}

TEST(OffsetMapping, ReserveAllNull) {
    OffsetMapping mapping;
    mapping.Reserve(5, 0, 1);
    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetValidCount(), 0);
    EXPECT_EQ(mapping.GetTotalCount(), 5);
}

// ---------- Build (eager) ----------

TEST(OffsetMapping, BuildBasicVecMode) {
    OffsetMapping mapping;
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
    OffsetMapping mapping;
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
    OffsetMapping mapping;
    std::vector<uint8_t> valid(4, 1);
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 4);
    EXPECT_EQ(mapping.GetValidCount(), 4);
    for (int64_t i = 0; i < 4; ++i) {
        EXPECT_EQ(mapping.GetPhysicalOffset(i), i);
        EXPECT_EQ(mapping.GetLogicalOffset(i), i);
    }
}

TEST(OffsetMapping, BuildAllNull) {
    OffsetMapping mapping;
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
    OffsetMapping mapping;
    mapping.Build(nullptr, 100);
    EXPECT_FALSE(mapping.IsEnabled());
    std::vector<uint8_t> valid(1, 1);
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 0);
    EXPECT_FALSE(mapping.IsEnabled());
}

TEST(OffsetMapping, BuildTwiceResetsState) {
    OffsetMapping mapping;
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

// Regression: Reserve followed by Build must not double-count valid_count.
TEST(OffsetMapping, ReserveThenBuildDoesNotDouble) {
    OffsetMapping mapping;
    mapping.Reserve(10, 6, 2);
    EXPECT_EQ(mapping.GetValidCount(), 6);

    auto valid = ToBoolBytes(MakeValid({1, 1, 0, 0, 1, 0, 1, 1, 0, 1}));
    mapping.Build(reinterpret_cast<const bool*>(valid.data()), 10);

    EXPECT_EQ(mapping.GetValidCount(), 6);
    EXPECT_EQ(mapping.GetTotalCount(), 10);
}

// ---------- SetChunk ----------

TEST(OffsetMapping, SetChunkVecModeSingleChunk) {
    OffsetMapping mapping;
    mapping.Reserve(5, 3, 1);
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 0, 1}));
    mapping.SetChunk(0, 0, 0, reinterpret_cast<const bool*>(valid.data()), 5);

    EXPECT_TRUE(mapping.IsChunkSet(0));
    EXPECT_EQ(mapping.GetValidCount(), 3);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(1), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(3), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(4), 2);
    EXPECT_EQ(mapping.GetLogicalOffset(0), 0);
    EXPECT_EQ(mapping.GetLogicalOffset(1), 2);
    EXPECT_EQ(mapping.GetLogicalOffset(2), 4);
}

TEST(OffsetMapping, SetChunkVecModeMultipleChunksInOrder) {
    OffsetMapping mapping;
    mapping.Reserve(10, 6, 2);

    auto c0 = ToBoolBytes(MakeValid({1, 1, 0, 0, 1}));
    auto c1 = ToBoolBytes(MakeValid({0, 1, 1, 0, 1}));

    mapping.SetChunk(0, 0, 0, reinterpret_cast<const bool*>(c0.data()), 5);
    mapping.SetChunk(1, 5, 3, reinterpret_cast<const bool*>(c1.data()), 5);

    EXPECT_TRUE(mapping.IsChunkSet(0));
    EXPECT_TRUE(mapping.IsChunkSet(1));
    EXPECT_EQ(mapping.GetValidCount(), 6);

    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(1), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(4), 2);
    EXPECT_EQ(mapping.GetPhysicalOffset(5), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(6), 3);
    EXPECT_EQ(mapping.GetPhysicalOffset(7), 4);
    EXPECT_EQ(mapping.GetPhysicalOffset(9), 5);
    for (int64_t p = 0; p < 6; ++p) {
        auto l = mapping.GetLogicalOffset(p);
        EXPECT_GE(l, 0);
        EXPECT_EQ(mapping.GetPhysicalOffset(l), p);
    }
}

TEST(OffsetMapping, SetChunkVecModeOutOfOrder) {
    OffsetMapping mapping;
    mapping.Reserve(10, 6, 2);

    auto c0 = ToBoolBytes(MakeValid({1, 1, 0, 0, 1}));
    auto c1 = ToBoolBytes(MakeValid({0, 1, 1, 0, 1}));

    mapping.SetChunk(1, 5, 3, reinterpret_cast<const bool*>(c1.data()), 5);
    EXPECT_FALSE(mapping.IsChunkSet(0));
    EXPECT_EQ(mapping.GetPhysicalOffset(6), 3);
    EXPECT_EQ(mapping.GetLogicalOffset(3), 6);

    mapping.SetChunk(0, 0, 0, reinterpret_cast<const bool*>(c0.data()), 5);
    EXPECT_TRUE(mapping.IsChunkSet(0));
    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetLogicalOffset(0), 0);
    EXPECT_EQ(mapping.GetValidCount(), 6);
}

TEST(OffsetMapping, SetChunkMapMode) {
    OffsetMapping mapping;
    mapping.Reserve(100, 2, 2);

    std::vector<uint8_t> c0(50, 0);
    c0[10] = 1;
    std::vector<uint8_t> c1(50, 0);
    c1[20] = 1;

    mapping.SetChunk(0, 0, 0, reinterpret_cast<const bool*>(c0.data()), 50);
    mapping.SetChunk(1, 50, 1, reinterpret_cast<const bool*>(c1.data()), 50);

    EXPECT_TRUE(mapping.IsChunkSet(0));
    EXPECT_TRUE(mapping.IsChunkSet(1));
    EXPECT_EQ(mapping.GetValidCount(), 2);
    EXPECT_EQ(mapping.GetPhysicalOffset(10), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(70), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(0), 10);
    EXPECT_EQ(mapping.GetLogicalOffset(1), 70);
}

TEST(OffsetMapping, SetChunkIdempotent) {
    OffsetMapping mapping;
    mapping.Reserve(5, 3, 1);
    auto valid = ToBoolBytes(MakeValid({1, 0, 1, 0, 1}));
    mapping.SetChunk(0, 0, 0, reinterpret_cast<const bool*>(valid.data()), 5);
    auto before = mapping.GetValidCount();
    mapping.SetChunk(0, 0, 0, reinterpret_cast<const bool*>(valid.data()), 5);
    EXPECT_EQ(mapping.GetValidCount(), before);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(2), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(4), 2);
}

TEST(OffsetMapping, ConcurrentSetChunkDifferentChunks) {
    OffsetMapping mapping;
    constexpr int kChunks = 8;
    constexpr int kRowsPerChunk = 32;
    constexpr int kTotal = kChunks * kRowsPerChunk;
    mapping.Reserve(kTotal, kTotal / 2, kChunks);

    std::vector<std::vector<uint8_t>> valids(
        kChunks, std::vector<uint8_t>(kRowsPerChunk));
    for (int c = 0; c < kChunks; ++c) {
        for (int r = 0; r < kRowsPerChunk; ++r) {
            valids[c][r] = (r % 2 == 0) ? 1 : 0;
        }
    }

    std::vector<std::thread> threads;
    threads.reserve(kChunks);
    for (int c = 0; c < kChunks; ++c) {
        threads.emplace_back([&, c] {
            int64_t start_logical = c * kRowsPerChunk;
            int64_t start_physical = c * (kRowsPerChunk / 2);
            mapping.SetChunk(c,
                             start_logical,
                             start_physical,
                             reinterpret_cast<const bool*>(valids[c].data()),
                             kRowsPerChunk);
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(mapping.GetValidCount(), kTotal / 2);
    for (int c = 0; c < kChunks; ++c) {
        EXPECT_TRUE(mapping.IsChunkSet(c));
    }
    for (int logical = 0; logical < kTotal; ++logical) {
        bool expect_valid = (logical % 2 == 0);
        int64_t phys = mapping.GetPhysicalOffset(logical);
        if (expect_valid) {
            EXPECT_GE(phys, 0) << "logical=" << logical;
            EXPECT_EQ(mapping.GetLogicalOffset(phys), logical);
        } else {
            EXPECT_EQ(phys, -1) << "logical=" << logical;
        }
    }
}

// ---------- Append ----------

TEST(OffsetMapping, AppendBasic) {
    OffsetMapping mapping;
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
    OffsetMapping mapping;
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

TEST(OffsetMapping, AppendConvertsMapModeToVec) {
    OffsetMapping mapping;
    std::vector<uint8_t> sparse(100, 0);
    sparse[7] = 1;
    mapping.Build(reinterpret_cast<const bool*>(sparse.data()), 100);
    EXPECT_EQ(mapping.GetValidCount(), 1);

    auto b = ToBoolBytes(MakeValid({1, 1, 0}));
    mapping.Append(reinterpret_cast<const bool*>(b.data()),
                   3,
                   mapping.GetTotalCount(),
                   mapping.GetValidCount());

    EXPECT_EQ(mapping.GetValidCount(), 3);
    EXPECT_EQ(mapping.GetTotalCount(), 103);
    EXPECT_EQ(mapping.GetPhysicalOffset(7), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(100), 1);
    EXPECT_EQ(mapping.GetPhysicalOffset(101), 2);
    EXPECT_EQ(mapping.GetLogicalOffset(0), 7);
    EXPECT_EQ(mapping.GetLogicalOffset(2), 101);
}

TEST(OffsetMapping, AppendNoopOnNullOrZero) {
    OffsetMapping mapping;
    mapping.Append(nullptr, 3, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
    std::vector<uint8_t> v(1, 1);
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 0, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
}

// ---------- IsValid ----------

TEST(OffsetMapping, IsValidMatchesPhysicalOffsetSign) {
    OffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 0}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 4);
    EXPECT_TRUE(mapping.IsValid(0));
    EXPECT_FALSE(mapping.IsValid(1));
    EXPECT_TRUE(mapping.IsValid(2));
    EXPECT_FALSE(mapping.IsValid(3));
}

// ---------- Out-of-bounds queries ----------

TEST(OffsetMapping, OutOfBoundsReturnsMinusOne) {
    OffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Build(reinterpret_cast<const bool*>(v.data()), 3);
    EXPECT_EQ(mapping.GetPhysicalOffset(99), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(99), -1);
}

}  // namespace milvus
