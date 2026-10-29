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

#include <algorithm>
#include <atomic>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include "common/GrowingOffsetMapping.h"

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

// ---------- Append ----------

TEST(GrowingOffsetMapping, AppendBasic) {
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

TEST(GrowingOffsetMapping, AppendMultipleBatches) {
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

TEST(GrowingOffsetMapping, AppendNoopOnNullOrZero) {
    GrowingOffsetMapping mapping;
    mapping.Append(nullptr, 3, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
    std::vector<uint8_t> v(1, 1);
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 0, 0, 0);
    EXPECT_FALSE(mapping.IsEnabled());
}

TEST(GrowingOffsetMapping, AppendRejectsNonContiguousLogicalBatch) {
    GrowingOffsetMapping mapping;
    auto first = ToBoolBytes(MakeValid({1, 1}));
    mapping.Append(reinterpret_cast<const bool*>(first.data()), 2, 0, 0);

    auto non_contiguous = ToBoolBytes(MakeValid({1}));
    EXPECT_ANY_THROW(mapping.Append(
        reinterpret_cast<const bool*>(non_contiguous.data()), 1, 3, 2));
    EXPECT_ANY_THROW(mapping.Append(
        reinterpret_cast<const bool*>(non_contiguous.data()), 1, 1, 2));
}

// ---------- ValidCountBelow ----------

// logical: 0(v) 1(x) 2(v) 3(v) 4(x) 5(v)
// physical:  0        1    2         3
TEST(GrowingOffsetMapping, ValidCountBelowConvertsLogicalBound) {
    GrowingOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 1, 0, 1}));
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 6, 0, 0);
    ASSERT_EQ(mapping.GetValidCount(), 4);
    ASSERT_EQ(mapping.GetTotalCount(), 6);

    EXPECT_EQ(mapping.ValidCountBelow(0), 0);
    EXPECT_EQ(mapping.ValidCountBelow(1), 1);  // {0}
    EXPECT_EQ(mapping.ValidCountBelow(2), 1);  // logical 1 is null
    EXPECT_EQ(mapping.ValidCountBelow(3), 2);  // {0,2}
    EXPECT_EQ(mapping.ValidCountBelow(4), 3);  // {0,2,3}
    EXPECT_EQ(mapping.ValidCountBelow(5), 3);  // logical 4 is null
    EXPECT_EQ(mapping.ValidCountBelow(6), 4);  // all
    // Bounds outside the mapping clamp instead of over-reporting.
    EXPECT_EQ(mapping.ValidCountBelow(-1), 0);
    EXPECT_EQ(mapping.ValidCountBelow(100), 4);
}

// The reason this API exists: a concurrent insert grows the mapping after a
// query has fixed its visible-row bound. The scan bound must reflect the
// bound the query was planned with, not the mapping's current size.
TEST(GrowingOffsetMapping, ValidCountBelowIgnoresRowsAppendedAfterBound) {
    GrowingOffsetMapping mapping;
    auto first = ToBoolBytes(MakeValid({1, 0, 1, 1}));
    mapping.Append(reinterpret_cast<const bool*>(first.data()), 4, 0, 0);

    // The query is planned here: 4 logical rows are acknowledged/visible.
    const int64_t planned_bound = mapping.GetTotalCount();
    const int64_t planned_physical = mapping.ValidCountBelow(planned_bound);
    ASSERT_EQ(planned_physical, 3);

    // A concurrent insert publishes more rows into the mapping before the
    // search kernel picks its scan range.
    auto second = ToBoolBytes(MakeValid({1, 1, 1, 1}));
    mapping.Append(reinterpret_cast<const bool*>(second.data()),
                   4,
                   mapping.GetTotalCount(),
                   mapping.GetValidCount());
    ASSERT_EQ(mapping.GetValidCount(), 7);

    // GetValidCount() would hand the search 7 physical rows, four of which are
    // not visible to this query; the converted bound stays at 3.
    EXPECT_EQ(mapping.ValidCountBelow(planned_bound), planned_physical);
    EXPECT_NE(mapping.GetValidCount(), planned_physical);

    // Every physical offset within the converted bound maps back below the
    // query's logical bound, which is exactly what Reduce asserts.
    for (int64_t physical = 0; physical < planned_physical; ++physical) {
        EXPECT_LT(mapping.GetLogicalOffset(physical), planned_bound);
    }
}

// A mapping that has never been appended to is disabled, so the bound passes
// through unchanged.
TEST(GrowingOffsetMapping, ValidCountBelowIsIdentityBeforeAppend) {
    GrowingOffsetMapping mapping;
    ASSERT_FALSE(mapping.IsEnabled());
    EXPECT_EQ(mapping.ValidCountBelow(42), 42);
    EXPECT_EQ(mapping.ValidCountBelow(-5), 0);
}

// Disabled means logical and physical spaces coincide -- for EVERY
// conversion. Rejecting offsets against total_count_ == 0 here would break
// the uniform contract the other implementations (and this class's own
// GetPhysicalOffset) honor.
TEST(GrowingOffsetMapping, FilterValidLogicalOffsetsIsIdentityWhenDisabled) {
    GrowingOffsetMapping mapping;
    ASSERT_FALSE(mapping.IsEnabled());

    const int64_t logical[] = {0, -1, 5};
    bool valid[3] = {};
    std::vector<int64_t> physical;
    mapping.FilterValidLogicalOffsets(logical, 3, valid, physical);

    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_TRUE(valid[2]);
    EXPECT_EQ(physical, (std::vector<int64_t>{0, 5}));
}

// The batch conversion gallops from a cursor on ascending inputs (the shape
// flush and chunk views produce). Verify it against the point-lookup answer
// on a sparse mapping large enough to cross chunk boundaries.
TEST(GrowingOffsetMapping, FilterValidLogicalOffsetsMatchesPointLookups) {
    GrowingOffsetMapping mapping;
    constexpr int64_t kRows = 5000;
    std::vector<uint8_t> valid(kRows);
    for (int64_t i = 0; i < kRows; ++i) {
        valid[i] = ((i % 7) == 0 || (i % 3) == 2) ? 1 : 0;
    }
    mapping.Append(reinterpret_cast<const bool*>(valid.data()), kRows, 0, 0);
    ASSERT_LT(mapping.GetValidCount(), kRows);

    // Full ascending range, with out-of-range offsets at both ends.
    std::vector<int64_t> logical(kRows + 2);
    std::iota(logical.begin(), logical.end(), int64_t{-1});
    auto flags = std::make_unique<bool[]>(logical.size());
    std::vector<int64_t> physical;
    mapping.FilterValidLogicalOffsets(
        logical.data(), logical.size(), flags.get(), physical);

    size_t k = 0;
    for (size_t i = 0; i < logical.size(); ++i) {
        const auto expected = mapping.GetPhysicalOffset(logical[i]);
        ASSERT_EQ(flags[i], expected >= 0) << "logical=" << logical[i];
        if (flags[i]) {
            ASSERT_EQ(physical[k++], expected) << "logical=" << logical[i];
        }
    }
    ASSERT_EQ(k, physical.size());
}

// Out-of-order and duplicate offsets fall back to fresh searches; the cursor
// must never make a later answer wrong.
TEST(GrowingOffsetMapping, FilterValidLogicalOffsetsHandlesUnsortedInput) {
    GrowingOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1, 1, 0, 1}));
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 6, 0, 0);
    // logical -> physical: 0->0, 2->1, 3->2, 5->3

    const int64_t logical[] = {5, 5, 0, 3, 2, 1, 4, 3};
    bool flags[8] = {};
    std::vector<int64_t> physical;
    mapping.FilterValidLogicalOffsets(logical, 8, flags, physical);

    const bool expected_flags[] = {
        true, true, true, true, true, false, false, true};
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(flags[i], expected_flags[i]) << i;
    }
    EXPECT_EQ(physical, (std::vector<int64_t>{3, 3, 0, 2, 1, 2}));
}

// A nullable column that has not received a null yet maps as the identity;
// the fast path must still reject out-of-range offsets.
TEST(GrowingOffsetMapping, AllValidMappingIsIdentity) {
    GrowingOffsetMapping mapping;
    std::vector<uint8_t> all_valid(100, 1);
    mapping.Append(reinterpret_cast<const bool*>(all_valid.data()), 100, 0, 0);

    EXPECT_EQ(mapping.GetPhysicalOffset(0), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(99), 99);
    EXPECT_EQ(mapping.GetPhysicalOffset(100), -1);

    const int64_t logical[] = {-1, 0, 42, 99, 100};
    bool flags[5] = {};
    std::vector<int64_t> physical;
    mapping.FilterValidLogicalOffsets(logical, 5, flags, physical);
    EXPECT_FALSE(flags[0]);
    EXPECT_TRUE(flags[1]);
    EXPECT_TRUE(flags[2]);
    EXPECT_TRUE(flags[3]);
    EXPECT_FALSE(flags[4]);
    EXPECT_EQ(physical, (std::vector<int64_t>{0, 42, 99}));
}

// ---------- dense chunked storage ----------

TEST(GrowingOffsetMapping, NullRowHasNoPhysicalOffset) {
    GrowingOffsetMapping mapping;
    auto v = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Append(reinterpret_cast<const bool*>(v.data()), 3, 0, 0);

    EXPECT_EQ(mapping.GetPhysicalOffset(1), -1);
    EXPECT_FALSE(mapping.IsValid(1));
    // Out of range in either space, in either direction.
    EXPECT_EQ(mapping.GetPhysicalOffset(3), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(-1), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(2), -1);
    EXPECT_EQ(mapping.GetLogicalOffset(-1), -1);
}

TEST(GrowingOffsetMapping, AppendAllNullBatchLeavesPhysicalSpaceEmpty) {
    GrowingOffsetMapping mapping;
    std::vector<uint8_t> all_null(5, 0);
    mapping.Append(reinterpret_cast<const bool*>(all_null.data()), 5, 0, 0);

    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetTotalCount(), 5);
    EXPECT_EQ(mapping.GetValidCount(), 0);
    EXPECT_EQ(mapping.ValidCountBelow(5), 0);
    for (int64_t logical = 0; logical < 5; ++logical) {
        EXPECT_EQ(mapping.GetPhysicalOffset(logical), -1) << logical;
    }
}

// Schema-evolution backfill appends every pre-existing row of a new nullable
// vector field as one all-null batch (an entire segment's worth). Point
// lookups over that prefix must all miss, and real inserts arriving after it
// must map correctly on top of the null prefix.
TEST(GrowingOffsetMapping, AllNullBackfillThenValidTail) {
    GrowingOffsetMapping mapping;
    constexpr int64_t kBackfill = 5000;
    std::vector<uint8_t> nulls(kBackfill, 0);
    mapping.Append(
        reinterpret_cast<const bool*>(nulls.data()), kBackfill, 0, 0);
    EXPECT_TRUE(mapping.IsEnabled());
    EXPECT_EQ(mapping.GetTotalCount(), kBackfill);
    EXPECT_EQ(mapping.GetValidCount(), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(0), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(kBackfill - 1), -1);
    EXPECT_EQ(mapping.ValidCountBelow(kBackfill), 0);

    auto tail = ToBoolBytes(MakeValid({1, 0, 1}));
    mapping.Append(reinterpret_cast<const bool*>(tail.data()), 3, kBackfill, 0);
    EXPECT_EQ(mapping.GetTotalCount(), kBackfill + 3);
    EXPECT_EQ(mapping.GetValidCount(), 2);
    EXPECT_EQ(mapping.GetPhysicalOffset(kBackfill), 0);
    EXPECT_EQ(mapping.GetPhysicalOffset(kBackfill + 1), -1);
    EXPECT_EQ(mapping.GetPhysicalOffset(kBackfill + 2), 1);
    EXPECT_EQ(mapping.GetLogicalOffset(1), kBackfill + 2);
    EXPECT_EQ(mapping.ValidCountBelow(kBackfill + 1), 1);
}

// Storage is a spine of geometrically growing chunks (1024, 2048, 4096, ...).
// Walk well past the first few boundaries, in batches that straddle them, and
// check both directions still agree.
TEST(GrowingOffsetMapping, AppendGrowsAcrossChunkBoundaries) {
    constexpr int64_t kRows = 20000;  // spans chunks 0..4
    constexpr int64_t kBatch = 97;    // deliberately not a chunk divisor

    GrowingOffsetMapping mapping;
    std::vector<int64_t> expected_p2l;
    std::vector<int64_t> expected_l2p(kRows, -1);

    for (int64_t start = 0; start < kRows; start += kBatch) {
        const int64_t count = std::min(kBatch, kRows - start);
        std::vector<uint8_t> valid(count);
        for (int64_t i = 0; i < count; ++i) {
            // Every third row is null, so logical and physical drift steadily.
            const bool is_valid = ((start + i) % 3) != 1;
            valid[i] = is_valid ? 1 : 0;
            if (is_valid) {
                expected_l2p[start + i] =
                    static_cast<int64_t>(expected_p2l.size());
                expected_p2l.push_back(start + i);
            }
        }
        mapping.Append(reinterpret_cast<const bool*>(valid.data()),
                       count,
                       mapping.GetTotalCount(),
                       mapping.GetValidCount());
    }

    ASSERT_EQ(mapping.GetTotalCount(), kRows);
    ASSERT_EQ(mapping.GetValidCount(),
              static_cast<int64_t>(expected_p2l.size()));

    for (int64_t logical = 0; logical < kRows; ++logical) {
        ASSERT_EQ(mapping.GetPhysicalOffset(logical), expected_l2p[logical])
            << "logical=" << logical;
    }
    for (size_t physical = 0; physical < expected_p2l.size(); ++physical) {
        ASSERT_EQ(mapping.GetLogicalOffset(static_cast<int64_t>(physical)),
                  expected_p2l[physical])
            << "physical=" << physical;
    }
    // ValidCountBelow binary searches the same array; check it at every chunk
    // boundary plus the endpoints.
    for (int64_t bound : {int64_t{0},
                          int64_t{1},
                          int64_t{1023},
                          int64_t{1024},
                          int64_t{3071},
                          int64_t{3072},
                          int64_t{7168},
                          kRows - 1,
                          kRows}) {
        const auto expected = static_cast<int64_t>(
            std::lower_bound(expected_p2l.begin(), expected_p2l.end(), bound) -
            expected_p2l.begin());
        ASSERT_EQ(mapping.ValidCountBelow(bound), expected)
            << "bound=" << bound;
    }
}

TEST(GrowingOffsetMapping,
     PhysicalToLogicalIdViewStopsAtInternalChunkBoundary) {
    constexpr int64_t kRows = 1700;

    GrowingOffsetMapping mapping;
    std::vector<int64_t> expected_p2l;
    std::vector<uint8_t> valid(kRows);
    for (int64_t i = 0; i < kRows; ++i) {
        const bool is_valid = i % 7 != 3;
        valid[i] = is_valid ? 1 : 0;
        if (is_valid) {
            expected_p2l.push_back(i);
        }
    }
    ASSERT_GT(expected_p2l.size(), 1124);
    mapping.Append(reinterpret_cast<const bool*>(valid.data()), kRows, 0, 0);

    auto tail = mapping.GetPhysicalToLogicalIds(1000, 100);
    ASSERT_NE(tail.data, nullptr);
    ASSERT_EQ(tail.count, 24);
    for (int64_t i = 0; i < tail.count; ++i) {
        EXPECT_EQ(tail.data[i], expected_p2l[1000 + i]) << "i=" << i;
    }

    auto next = mapping.GetPhysicalToLogicalIds(1024, 100);
    ASSERT_NE(next.data, nullptr);
    ASSERT_EQ(next.count, 100);
    for (int64_t i = 0; i < next.count; ++i) {
        EXPECT_EQ(next.data[i], expected_p2l[1024 + i]) << "i=" << i;
    }
}

// ---------- concurrency ----------

// One writer, several lock-free readers -- the mapping's actual runtime shape.
// Every reader must see a self-consistent snapshot: the counts it reads, the
// bound it derives from them, and the entries below that bound all come from
// one generation. Run this under TSan to also cover the publication ordering.
TEST(GrowingOffsetMapping, ConcurrentReadersSeeAConsistentSnapshot) {
    // Enough to cross the 1024 / 3072 / 7168 chunk boundaries while a reader
    // is mid-scan, without making every outer iteration a long walk.
    constexpr int64_t kRows = 8000;
    constexpr int64_t kBatch = 61;
    constexpr int kReaders = 3;

    GrowingOffsetMapping mapping;
    std::atomic<bool> writer_done{false};
    std::atomic<int64_t> failures{0};

    std::vector<std::thread> readers;
    for (int r = 0; r < kReaders; ++r) {
        readers.emplace_back([&] {
            while (!writer_done.load(std::memory_order_acquire)) {
                // A query fixes its logical bound first, exactly as the plan
                // layer does, then converts it into physical space.
                const int64_t logical_bound = mapping.GetTotalCount();
                const int64_t physical_bound =
                    mapping.ValidCountBelow(logical_bound);
                if (physical_bound > mapping.GetValidCount()) {
                    failures.fetch_add(1);
                    return;
                }
                for (int64_t physical = 0; physical < physical_bound;
                     ++physical) {
                    const int64_t logical = mapping.GetLogicalOffset(physical);
                    // Reduce asserts exactly this: every offset a search can
                    // return maps back inside the query's row count.
                    if (logical < 0 || logical >= logical_bound ||
                        mapping.GetPhysicalOffset(logical) != physical) {
                        failures.fetch_add(1);
                        return;
                    }
                }
            }
        });
    }

    for (int64_t start = 0; start < kRows; start += kBatch) {
        const int64_t count = std::min(kBatch, kRows - start);
        std::vector<uint8_t> valid(count);
        for (int64_t i = 0; i < count; ++i) {
            valid[i] = (((start + i) % 4) == 3) ? 0 : 1;
        }
        mapping.Append(reinterpret_cast<const bool*>(valid.data()),
                       count,
                       mapping.GetTotalCount(),
                       mapping.GetValidCount());
    }
    writer_done.store(true, std::memory_order_release);
    for (auto& reader : readers) {
        reader.join();
    }

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(mapping.GetTotalCount(), kRows);
}

}  // namespace milvus
