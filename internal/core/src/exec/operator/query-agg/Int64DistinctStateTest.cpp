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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <random>
#include <vector>

#include <folly/CancellationToken.h>

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "exec/operator/query-agg/Int64DistinctState.h"

namespace milvus::exec {
namespace {

constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();
constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();

Int64DistinctStateLimits
Unlimited() {
    return {kInt64Max, kInt64Max};
}

// One input batch in the shape the state consumes. An empty bitmap vector
// means "pass nullptr" (non-nullable column / no exclusion).
struct Batch {
    std::vector<int64_t> values;
    std::vector<bool> valid;
    std::vector<bool> excluded;
};

TargetBitmap
ToBitmap(const std::vector<bool>& bits) {
    TargetBitmap bitmap(bits.size());
    for (size_t i = 0; i < bits.size(); ++i) {
        bitmap.set(i, bits[i]);
    }
    return bitmap;
}

void
Add(Int64DistinctState& state, const Batch& batch) {
    TargetBitmap valid = ToBitmap(batch.valid);
    TargetBitmap excluded = ToBitmap(batch.excluded);
    TargetBitmapView valid_view(valid);
    TargetBitmapView excluded_view(excluded);
    state.AddBatch(batch.values.data(),
                   static_cast<int64_t>(batch.values.size()),
                   batch.valid.empty() ? nullptr : &valid_view,
                   batch.excluded.empty() ? nullptr : &excluded_view);
}

// Sort/unique oracle over the counted rows of all batches.
Int64DistinctSnapshot
Oracle(const std::vector<Batch>& batches) {
    Int64DistinctSnapshot expected;
    for (const auto& batch : batches) {
        for (size_t i = 0; i < batch.values.size(); ++i) {
            if (!batch.excluded.empty() && batch.excluded[i]) {
                continue;
            }
            if (!batch.valid.empty() && !batch.valid[i]) {
                expected.has_null = true;
                continue;
            }
            expected.values.push_back(batch.values[i]);
        }
    }
    std::sort(expected.values.begin(), expected.values.end());
    expected.values.erase(
        std::unique(expected.values.begin(), expected.values.end()),
        expected.values.end());
    return expected;
}

void
ExpectMatchesOracle(const Int64DistinctState& state,
                    const std::vector<Batch>& batches) {
    const auto expected = Oracle(batches);
    const auto actual = state.ExportSnapshot();
    EXPECT_EQ(actual.values, expected.values);
    EXPECT_EQ(actual.has_null, expected.has_null);
    EXPECT_EQ(state.HasNull(), expected.has_null);
    EXPECT_EQ(state.NonNullCount(),
              static_cast<int64_t>(expected.values.size()));
    EXPECT_EQ(state.DistinctCount(),
              static_cast<int64_t>(expected.values.size()) +
                  (expected.has_null ? 1 : 0));
    // Values() holds the same set as the snapshot, in first-seen order.
    auto unsorted = state.Values();
    std::sort(unsorted.begin(), unsorted.end());
    EXPECT_EQ(unsorted, expected.values);
}

Batch
RandomBatch(std::mt19937_64& rng,
            int64_t rows,
            int64_t domain,
            double null_rate,
            double excluded_rate) {
    std::uniform_int_distribution<int64_t> pick(0, domain - 1);
    std::uniform_real_distribution<double> coin(0.0, 1.0);
    const bool nullable = coin(rng) < 0.7;
    const bool filtered = coin(rng) < 0.7;
    Batch batch;
    for (int64_t i = 0; i < rows; ++i) {
        // Spread values over the full Int64 range, extrema included.
        const int64_t slot = pick(rng);
        int64_t value = slot == 0   ? kInt64Min
                        : slot == 1 ? kInt64Max
                                    : (slot - domain / 2) * 7919;
        batch.values.push_back(value);
        if (nullable) {
            batch.valid.push_back(coin(rng) >= null_rate);
        }
        if (filtered) {
            batch.excluded.push_back(coin(rng) < excluded_rate);
        }
    }
    return batch;
}

ErrorCode
CodeOf(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const SegcoreError& e) {
        return e.get_error_code();
    }
    return ErrorCode::Success;
}

}  // namespace

TEST(Int64DistinctStateTest, EmptyInput) {
    Int64DistinctState state(Unlimited());
    ExpectMatchesOracle(state, {});
    // An empty batch, with or without values or bitmaps, changes nothing.
    state.AddBatch(nullptr, 0, nullptr, nullptr);
    Add(state, Batch{{}, {}, {}});
    EXPECT_EQ(state.DistinctCount(), 0);
    EXPECT_FALSE(state.HasNull());
    EXPECT_TRUE(state.Values().empty());
    EXPECT_EQ(state.MemoryUsageBytes(),
              Int64DistinctState::ProjectedBytesForCapacity(0));
}

TEST(Int64DistinctStateTest, RepeatedValuesAndExtrema) {
    std::vector<Batch> batches = {
        {{5, 5, kInt64Min, 0, kInt64Max, 5, -1, 0}, {}, {}},
        {{kInt64Max, kInt64Min, -1, 7}, {}, {}},
    };
    Int64DistinctState state(Unlimited());
    for (const auto& batch : batches) {
        Add(state, batch);
    }
    ExpectMatchesOracle(state, batches);
    EXPECT_EQ(state.ExportSnapshot().values,
              (std::vector<int64_t>{kInt64Min, -1, 0, 5, 7, kInt64Max}));
}

TEST(Int64DistinctStateTest, DuplicatesAcrossBatches) {
    std::vector<Batch> batches = {
        {{1, 2, 3}, {}, {}},
        {{3, 2, 1}, {}, {}},
        {{4, 1}, {}, {}},
    };
    Int64DistinctState state(Unlimited());
    for (const auto& batch : batches) {
        Add(state, batch);
        ExpectMatchesOracle(
            state,
            std::vector<Batch>(
                batches.begin(),
                batches.begin() + (&batch - batches.data()) + 1));
    }
    EXPECT_EQ(state.DistinctCount(), 4);
}

TEST(Int64DistinctStateTest, NullsCollapseToOne) {
    // The value under a NULL row is garbage and must not be counted.
    std::vector<Batch> batches = {
        {{10, 99, 10}, {true, false, true}, {}},
        {{98, 97}, {false, false}, {}},
        {{11}, {true}, {}},
    };
    Int64DistinctState state(Unlimited());
    for (const auto& batch : batches) {
        Add(state, batch);
    }
    ExpectMatchesOracle(state, batches);
    EXPECT_TRUE(state.HasNull());
    EXPECT_EQ(state.NonNullCount(), 2);
    EXPECT_EQ(state.DistinctCount(), 3);
}

TEST(Int64DistinctStateTest, AllNullInput) {
    std::vector<Batch> batches = {{{1, 2, 3}, {false, false, false}, {}}};
    Int64DistinctState state(Unlimited());
    Add(state, batches[0]);
    ExpectMatchesOracle(state, batches);
    EXPECT_EQ(state.NonNullCount(), 0);
    EXPECT_EQ(state.DistinctCount(), 1);
}

TEST(Int64DistinctStateTest, ExcludedRowsAreSkipped) {
    // Excluded rows contribute neither values nor NULL.
    std::vector<Batch> batches = {
        {{1, 2, 3, 4}, {true, false, true, true}, {false, true, true, false}},
    };
    Int64DistinctState state(Unlimited());
    Add(state, batches[0]);
    ExpectMatchesOracle(state, batches);
    EXPECT_FALSE(state.HasNull());
    EXPECT_EQ(state.ExportSnapshot().values, (std::vector<int64_t>{1, 4}));
}

TEST(Int64DistinctStateTest, AllRowsExcluded) {
    std::vector<Batch> batches = {
        {{1, 2, 3}, {false, true, true}, {true, true, true}},
        {{4, 5}, {}, {true, true}},
    };
    Int64DistinctState state(Unlimited());
    for (const auto& batch : batches) {
        Add(state, batch);
    }
    ExpectMatchesOracle(state, batches);
    EXPECT_EQ(state.DistinctCount(), 0);
}

TEST(Int64DistinctStateTest, RandomBatchesMatchOracle) {
    std::mt19937_64 rng(53848);
    for (int round = 0; round < 50; ++round) {
        std::uniform_int_distribution<int64_t> batch_count(0, 8);
        std::uniform_int_distribution<int64_t> rows(0, 3000);
        std::uniform_int_distribution<int64_t> domain(2, 5000);
        std::vector<Batch> batches;
        Int64DistinctState state(Unlimited());
        const auto n = batch_count(rng);
        for (int64_t b = 0; b < n; ++b) {
            batches.push_back(
                RandomBatch(rng, rows(rng), domain(rng), 0.1, 0.3));
            Add(state, batches.back());
        }
        ExpectMatchesOracle(state, batches);
    }
}

TEST(Int64DistinctStateTest, MemoryUsageFollowsProjection) {
    Int64DistinctState state(Unlimited());
    int64_t capacity = Int64DistinctState::kInitialCapacity;
    std::vector<int64_t> values;
    for (int64_t v = 0; v < 5000; ++v) {
        values.push_back(v);
        state.AddBatch(&values.back(), 1, nullptr, nullptr);
        if (v + 1 > capacity) {
            capacity *= 2;
        }
        ASSERT_EQ(state.MemoryUsageBytes(),
                  Int64DistinctState::ProjectedBytesForCapacity(capacity))
            << "after " << v + 1 << " values";
    }
}

TEST(Int64DistinctStateTest, EntryLimitIsExplicit) {
    Int64DistinctState state({3, kInt64Max});
    Add(state, Batch{{1, 2, 3, 3, 2, 1}, {}, {}});
    // Repeats and NULL never consume the entry budget.
    Add(state, Batch{{1, 0}, {true, false}, {}});
    EXPECT_EQ(state.DistinctCount(), 4);

    EXPECT_EQ(CodeOf([&] {
                  Add(state, Batch{{2, 4}, {}, {}});
              }),
              ErrorCode::InsufficientResource);
    EXPECT_TRUE(state.Failed());
    // A failed state cannot be read as an exact result.
    EXPECT_EQ(CodeOf([&] { state.DistinctCount(); }),
              ErrorCode::UnexpectedError);
    EXPECT_EQ(CodeOf([&] { state.ExportSnapshot(); }),
              ErrorCode::UnexpectedError);
    EXPECT_EQ(CodeOf([&] {
                  Add(state, Batch{{1}, {}, {}});
              }),
              ErrorCode::UnexpectedError);
    EXPECT_EQ(state.MemoryUsageBytes(),
              Int64DistinctState::ProjectedBytesForCapacity(0));
}

TEST(Int64DistinctStateTest, ZeroEntryLimitAcceptsOnlyNullAndExcluded) {
    Int64DistinctState state({0, kInt64Max});
    Add(state, Batch{{1, 2}, {false, true}, {false, true}});
    EXPECT_EQ(state.DistinctCount(), 1);
    EXPECT_EQ(CodeOf([&] {
                  Add(state, Batch{{3}, {}, {}});
              }),
              ErrorCode::InsufficientResource);
}

TEST(Int64DistinctStateTest, MemoryLimitIsExplicitAndRespected) {
    const int64_t budget = Int64DistinctState::ProjectedBytesForCapacity(100);
    Int64DistinctState state({kInt64Max, budget});
    std::vector<int64_t> values;
    for (int64_t v = 0; v < 100; ++v) {
        values.push_back(v * 3);
    }
    // The budget is used fully: exactly 100 values fit.
    Add(state, Batch{values, {}, {}});
    EXPECT_EQ(state.NonNullCount(), 100);
    EXPECT_LE(state.MemoryUsageBytes(), budget);
    Add(state, Batch{values, {}, {}});

    EXPECT_EQ(CodeOf([&] {
                  Add(state, Batch{{1}, {}, {}});
              }),
              ErrorCode::InsufficientResource);
    EXPECT_TRUE(state.Failed());
    EXPECT_LE(state.MemoryUsageBytes(), budget);
}

TEST(Int64DistinctStateTest, MemoryNeverExceedsBudgetUnderRandomInput) {
    std::mt19937_64 rng(1797);
    for (int round = 0; round < 30; ++round) {
        std::uniform_int_distribution<int64_t> cap(0, 4000);
        const int64_t budget =
            Int64DistinctState::ProjectedBytesForCapacity(cap(rng));
        Int64DistinctState state({kInt64Max, budget});
        std::vector<Batch> batches;
        bool rejected = false;
        for (int b = 0; b < 6 && !rejected; ++b) {
            batches.push_back(RandomBatch(rng, 1500, 8000, 0.05, 0.2));
            const auto code = CodeOf([&] { Add(state, batches.back()); });
            ASSERT_LE(state.MemoryUsageBytes(), budget);
            if (code != ErrorCode::Success) {
                ASSERT_EQ(code, ErrorCode::InsufficientResource);
                // Rejected only when the exact answer really does not fit.
                ASSERT_GT(
                    Int64DistinctState::ProjectedBytesForCapacity(
                        static_cast<int64_t>(Oracle(batches).values.size())),
                    budget);
                rejected = true;
            }
        }
        if (!rejected) {
            ExpectMatchesOracle(state, batches);
        }
    }
}

TEST(Int64DistinctStateTest, CancellationBeforeBatch) {
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    Int64DistinctState state(Unlimited(), &op_ctx);
    Add(state, Batch{{1, 2}, {}, {}});
    EXPECT_EQ(state.DistinctCount(), 2);

    source.requestCancellation();
    EXPECT_EQ(CodeOf([&] {
                  Add(state, Batch{{3}, {}, {}});
              }),
              ErrorCode::FollyCancel);
    EXPECT_TRUE(state.Failed());
    EXPECT_EQ(CodeOf([&] { state.Values(); }), ErrorCode::UnexpectedError);
}

TEST(Int64DistinctStateTest, CancellationReleasesState) {
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    Int64DistinctState state(Unlimited(), &op_ctx);
    std::vector<int64_t> values(Int64DistinctState::kCancellationCheckInterval *
                                4);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<int64_t>(i);
    }
    state.AddBatch(
        values.data(), static_cast<int64_t>(values.size()), nullptr, nullptr);
    EXPECT_EQ(state.NonNullCount(), static_cast<int64_t>(values.size()));
    EXPECT_GT(state.MemoryUsageBytes(),
              Int64DistinctState::ProjectedBytesForCapacity(0));

    // Even an empty batch observes the cancellation, and the table memory is
    // released when it does.
    source.requestCancellation();
    EXPECT_EQ(CodeOf([&] { state.AddBatch(nullptr, 0, nullptr, nullptr); }),
              ErrorCode::FollyCancel);
    EXPECT_TRUE(state.Failed());
    EXPECT_EQ(state.MemoryUsageBytes(),
              Int64DistinctState::ProjectedBytesForCapacity(0));
}

TEST(Int64DistinctStateTest, RejectsMalformedBatches) {
    Int64DistinctState state(Unlimited());
    std::vector<int64_t> values = {1, 2, 3};
    TargetBitmap short_bitmap(2);
    TargetBitmapView short_view(short_bitmap);
    EXPECT_EQ(CodeOf([&] { state.AddBatch(nullptr, 3, nullptr, nullptr); }),
              ErrorCode::UnexpectedError);
    EXPECT_EQ(
        CodeOf([&] { state.AddBatch(values.data(), -1, nullptr, nullptr); }),
        ErrorCode::UnexpectedError);
    EXPECT_EQ(
        CodeOf([&] { state.AddBatch(values.data(), 3, &short_view, nullptr); }),
        ErrorCode::UnexpectedError);
    EXPECT_EQ(
        CodeOf([&] { state.AddBatch(values.data(), 3, nullptr, &short_view); }),
        ErrorCode::UnexpectedError);
    // Argument errors are detected before any row is consumed.
    EXPECT_FALSE(state.Failed());
    EXPECT_EQ(state.DistinctCount(), 0);
}

TEST(Int64DistinctStateTest, RejectsInvalidLimits) {
    EXPECT_EQ(CodeOf([] {
                  Int64DistinctState state({-1, kInt64Max});
              }),
              ErrorCode::UnexpectedError);
    EXPECT_EQ(CodeOf([] {
                  Int64DistinctState state(
                      {kInt64Max,
                       Int64DistinctState::ProjectedBytesForCapacity(0) - 1});
              }),
              ErrorCode::UnexpectedError);
}

// The exported values are themselves a valid input batch, so a later
// consumer (DT02, #53849) can union states without a new input format.
TEST(Int64DistinctStateTest, SnapshotReingestsAsUnion) {
    Int64DistinctState left(Unlimited());
    Int64DistinctState right(Unlimited());
    Add(left, Batch{{1, 2}, {}, {}});
    Add(right, Batch{{1, 3, 0}, {true, true, false}, {}});

    Int64DistinctState merged(Unlimited());
    for (const auto* state : {&left, &right}) {
        auto snapshot = state->ExportSnapshot();
        merged.AddBatch(snapshot.values.data(),
                        static_cast<int64_t>(snapshot.values.size()),
                        nullptr,
                        nullptr);
        if (snapshot.has_null) {
            TargetBitmap null_row(1);
            TargetBitmapView null_view(null_row);
            const int64_t ignored = 0;
            merged.AddBatch(&ignored, 1, &null_view, nullptr);
        }
    }
    EXPECT_EQ(merged.ExportSnapshot().values, (std::vector<int64_t>{1, 2, 3}));
    EXPECT_EQ(merged.NonNullCount(), 3);
    EXPECT_EQ(merged.DistinctCount(), 4);
}

}  // namespace milvus::exec
