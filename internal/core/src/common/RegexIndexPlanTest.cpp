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

#include <cmath>
#include <limits>
#include <string>
#include <unordered_map>

#include "common/RegexIndexPlan.h"

namespace milvus {
namespace {

using Condition = RegexLiteralCondition;

RegexIndexCostModel
LargeSegment() {
    RegexIndexCostModel model;
    model.rows = 1000000;
    model.text_bytes = 100000000;
    return model;
}

RegexIndexCostModel
LongRowSegment() {
    auto model = LargeSegment();
    model.text_bytes = 1000 * model.rows;
    return model;
}

Condition
Literal(const std::string& text) {
    return Condition::Literal(text);
}

TEST(RegexIndexPlan, KeepsCheapestAndChildAndEveryOrBranch) {
    // (foo OR bar) AND baz selects the complete OR, not just foo. Its
    // occurrence estimate (4+6) is cheaper than baz (12).
    const auto requirements = Condition::Combine(
        Condition::Op::And,
        Condition::Combine(Condition::Op::Or, Literal("foo"), Literal("bar")),
        Literal("baz"));
    const std::unordered_map<std::string, size_t> counts = {
        {"foo", 4}, {"bar", 6}, {"baz", 12}};
    std::unordered_map<std::string, size_t> calls;
    const auto plan = PrepareRegexIndexPlan(
        requirements,
        [&](const auto& literal) {
            ++calls[literal];
            return counts.at(literal);
        },
        LargeSegment());
    EXPECT_TRUE(plan.Usable());
    EXPECT_EQ(plan.condition.op, Condition::Op::Or);
    EXPECT_EQ(plan.occurrences, 10);
    EXPECT_EQ(plan.estimated_rows, 10);
    EXPECT_EQ(plan.literal_count, 2);
    EXPECT_EQ(plan.bitmap_operations, 1);
    for (const auto& [literal, count] : counts) {
        EXPECT_EQ(calls.at(literal), 1);
    }
    // A row satisfying either OR branch remains a candidate. Final baz and
    // regex semantics are deliberately left to canonical RE2 rechecking.
    EXPECT_TRUE(plan.condition.Evaluate(
        [](const auto& literal) { return literal == "bar"; },
        [] { return true; }));
}

TEST(RegexIndexPlan, CountsDistinctAtomsOnceAndReusesPreparation) {
    // Selection can leave a duplicate inside nested ORs. Preparation Counts
    // each atom once, while cost still reflects each actual Locate call.
    const Condition requirements = {
        Condition::Op::Or,
        {},
        {{Condition::Op::Or, {}, {Literal("foo"), Literal("bar")}},
         Literal("foo")}};
    size_t calls = 0;
    const auto plan = PrepareRegexIndexPlan(
        requirements,
        [&](const auto& literal) {
            ++calls;
            return literal == "foo" ? 4 : 6;
        },
        LargeSegment());
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(plan.occurrences, 14);
    EXPECT_EQ(plan.literal_count, 3);
    for (size_t i = 1; i <= 8192; i *= 2) {
        ShouldUseRegexIndexPlan(plan, 1000000, i, 8192);
    }
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(plan.occurrences, 14);
}

TEST(RegexIndexPlan, PrefilterCanDeclineAFormerlyUsefulLocate) {
    const auto model = LongRowSegment();
    const auto plan = PrepareRegexIndexPlan(
        Literal("common"), [](const auto&) { return 100000; }, model);
    EXPECT_TRUE(ShouldUseRegexIndexPlan(plan, model.rows, 8192, 8192));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, model.rows, 82, 8192));
    // Prefiltering never discounts the complete index's Locate work. An
    // erroneous Count*p estimate would make this sparse batch choose FM.
    EXPECT_GE(RegexIndexExecutionCost(plan, model.rows, 82, 8192), 200000000);
}

TEST(RegexIndexPlan, PrefilterNeverWidensTheOriginalLocateGuard) {
    auto model = LargeSegment();
    model.text_bytes = 28 * model.rows;
    const auto outside = PrepareRegexIndexPlan(
        Literal("common"), [](const auto&) { return 4000; }, model);
    // Added view/dispatch weights would accept this short-row case both
    // dense and at about 1%, but the original guard's threshold is C<3500.
    EXPECT_LT(RegexIndexExecutionCost(outside, model.rows, 8192, 8192),
              RegexIndexScanCost(model, model.rows, 8192, 8192));
    EXPECT_LT(RegexIndexExecutionCost(outside, model.rows, 82, 8192),
              RegexIndexScanCost(model, model.rows, 82, 8192));
    EXPECT_FALSE(RegexIndexPassesLocateGuard(outside));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(outside, model.rows, 8192, 8192));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(outside, model.rows, 82, 8192));
    EXPECT_FALSE(CanRegexIndexPlanEverHelp(outside, model.rows, 8192));

    // Inside the unchanged limit, the bitmap-aware gate remains effective.
    const auto long_rows = LongRowSegment();
    const auto inside = PrepareRegexIndexPlan(
        Literal("less_common"), [](const auto&) { return 100000; }, long_rows);
    EXPECT_TRUE(RegexIndexPassesLocateGuard(inside));
    EXPECT_TRUE(ShouldUseRegexIndexPlan(inside, long_rows.rows, 8192, 8192));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(inside, long_rows.rows, 82, 8192));
    EXPECT_TRUE(CanRegexIndexPlanEverHelp(inside, long_rows.rows, 8192));
}

TEST(RegexIndexPlan, SparseDeclineCanBeReconsideredForADenseBatch) {
    const auto model = LongRowSegment();
    const auto plan = PrepareRegexIndexPlan(
        Literal("common"), [](const auto&) { return 100000; }, model);
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, model.rows, 82, 8192));
    EXPECT_TRUE(ShouldUseRegexIndexPlan(plan, model.rows - 8192, 8192, 8192));
    // As the remaining scan window shrinks, whole-segment Locate is harder
    // to amortize even with a dense batch.
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, 8192, 8192, 8192));
}

TEST(RegexIndexPlan, FixedWorkCanProvePermanentFallback) {
    const auto model = LongRowSegment();
    const auto impossible = PrepareRegexIndexPlan(
        Literal("common"), [](const auto&) { return 1000000; }, model);
    EXPECT_FALSE(CanRegexIndexPlanEverHelp(impossible, model.rows, 8192));
    EXPECT_GT(RegexIndexFixedCost(impossible),
              RegexIndexScanCost(model, model.rows, 8192, 8192));

    const auto retryable = PrepareRegexIndexPlan(
        Literal("less_common"), [](const auto&) { return 100000; }, model);
    EXPECT_FALSE(ShouldUseRegexIndexPlan(retryable, model.rows, 82, 8192));
    EXPECT_TRUE(CanRegexIndexPlanEverHelp(retryable, model.rows, 8192));
    EXPECT_TRUE(
        ShouldUseRegexIndexPlan(retryable, model.rows - 8192, 8192, 8192));
    EXPECT_FALSE(CanRegexIndexPlanEverHelp(retryable, 8192, 8192));
}

TEST(RegexIndexPlan, CalibratedLocateRetainsMediumAndOrSparseWins) {
    auto model = LargeSegment();
    // The production paired benchmark has 32771 rows averaging 523 bytes.
    // Count is 513 for its medium literal and 522 for the complete OR.
    model.rows = 32771;
    model.text_bytes = 17147072;
    const auto count = [](const std::string& literal) {
        return literal == "medium" ? 513 : 9;
    };
    const auto medium = PrepareRegexIndexPlan(Literal("medium"), count, model);
    const auto alternatives = PrepareRegexIndexPlan(
        Condition::Combine(
            Condition::Op::Or, Literal("medium"), Literal("rare")),
        count,
        model);
    EXPECT_EQ(medium.occurrences, 513);
    EXPECT_EQ(alternatives.occurrences, 522);
    for (const auto* plan : {&medium, &alternatives}) {
        EXPECT_TRUE(RegexIndexPassesLocateGuard(*plan));
        EXPECT_TRUE(CanRegexIndexPlanEverHelp(*plan, model.rows, 8192));
        for (size_t active : {size_t{82}, size_t{820}, size_t{8192}}) {
            EXPECT_TRUE(
                ShouldUseRegexIndexPlan(*plan, model.rows, active, 8192));
        }
    }
    // The soft calibration cannot accept a Count rejected by the old guard.
    const auto common = PrepareRegexIndexPlan(
        Literal("common"), [](const auto&) { return 32771; }, model);
    EXPECT_FALSE(RegexIndexPassesLocateGuard(common));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(common, model.rows, 8192, 8192));
    EXPECT_FALSE(CanRegexIndexPlanEverHelp(common, model.rows, 8192));
}

TEST(RegexIndexPlan, RareCandidatesRemainUsefulAfterStrongPrefiltering) {
    const auto model = LargeSegment();
    const auto plan = PrepareRegexIndexPlan(
        Literal("rare"), [](const auto&) { return 100; }, model);
    EXPECT_TRUE(ShouldUseRegexIndexPlan(plan, model.rows, 82, 8192));
    const auto scan = RegexIndexScanCost(model, model.rows, 82, 8192);
    EXPECT_GT(scan, 32000000);
    EXPECT_LT(RegexIndexExecutionCost(plan, model.rows, 82, 8192), scan);
}

TEST(RegexIndexPlan, ExactVisitedBatchesDistinguishClusteredAndScatteredMasks) {
    auto model = LargeSegment();
    model.text_bytes = 28 * model.rows;
    const auto plan = PrepareRegexIndexPlan(
        Literal("MARKER54191"), [](const auto&) { return 2000; }, model);
    ASSERT_TRUE(RegexIndexPassesLocateGuard(plan));
    ASSERT_TRUE(CanRegexIndexPlanEverHelp(plan, model.rows, 8192));
    for (size_t active : {size_t{128}, size_t{512}}) {
        const RegexIndexWorkStats clustered{model.rows, active, 8192, 1};
        const RegexIndexWorkStats scattered{
            model.rows, active, model.rows, (model.rows + 8191) / 8192};
        // Both masks contain exactly the same number of active rows. Only
        // one physical window needs string views for the clustered prefix;
        // popcount alone cannot price that difference.
        EXPECT_TRUE(ShouldUseRegexIndexPlan(plan, model.rows, active, 8192));
        EXPECT_FALSE(ShouldUseRegexIndexPlan(
            plan, model.rows, active, 8192, &clustered));
        EXPECT_TRUE(ShouldUseRegexIndexPlan(
            plan, model.rows, active, 8192, &scattered));
        EXPECT_GT(
            RegexIndexFixedCost(plan),
            RegexIndexScanCost(model, model.rows, active, 8192, &clustered));
    }
    const RegexIndexWorkStats all_empty{model.rows, 0, 0, 0};
    EXPECT_FALSE(
        ShouldUseRegexIndexPlan(plan, model.rows, 128, 8192, &all_empty));
    // Complete logical output size is not the physical scan batch geometry.
    const RegexIndexWorkStats prefix{model.rows, 128, 8192, 1};
    EXPECT_FALSE(
        ShouldUseRegexIndexPlan(plan, model.rows, model.rows, 8192, &prefix));
}

TEST(RegexIndexPlan, UnknownOrMalformedWorkStatisticsRetainTheDensityEstimate) {
    auto model = LargeSegment();
    model.text_bytes = 28 * model.rows;
    const auto plan = PrepareRegexIndexPlan(
        Literal("MARKER54191"), [](const auto&) { return 2000; }, model);
    const auto unknown_cost = RegexIndexScanCost(model, model.rows, 128, 8192);
    for (const RegexIndexWorkStats invalid : {
             RegexIndexWorkStats{model.rows - 1, 128, 8192, 1},
             RegexIndexWorkStats{model.rows, 128, 128, 0},
             RegexIndexWorkStats{model.rows, 128, 127, 1},
             RegexIndexWorkStats{model.rows, 0, 8192, 1},
             RegexIndexWorkStats{model.rows, 128, model.rows + 1, 1},
             RegexIndexWorkStats{model.rows, 128, 8192, model.rows},
         }) {
        EXPECT_EQ(RegexIndexScanCost(model, model.rows, 128, 8192, &invalid),
                  unknown_cost);
        EXPECT_TRUE(
            ShouldUseRegexIndexPlan(plan, model.rows, 128, 8192, &invalid));
    }
}

TEST(RegexIndexPlan, FullDensityBudgetIncludesARealShortTailBatch) {
    auto model = LargeSegment();
    model.text_bytes = 523 * model.rows;
    const auto plan = PrepareRegexIndexPlan(
        Literal("marker"), [](const auto&) { return 2287; }, model);
    const RegexIndexWorkStats full_window{8194, 8194, 8194, 2};
    ASSERT_TRUE(RegexIndexPassesLocateGuard(plan));
    EXPECT_GE(RegexIndexScanCost(model, 8194, 8192, 8192),
              RegexIndexScanCost(model, 8194, 8194, 8192, &full_window));
    // The fixed work is barely below this two-batch maximum. A fractional
    // 8194/8192 dispatch budget would wrongly prove that FM can never help.
    EXPECT_TRUE(CanRegexIndexPlanEverHelp(plan, 8194, 8192));
}

TEST(RegexIndexPlan, PreparationGateAvoidsEmptyAndSmallScanWindows) {
    const auto model = LargeSegment();
    EXPECT_FALSE(ShouldPrepareRegexIndexPlan(model, model.rows, 0, 8192));
    EXPECT_FALSE(ShouldPrepareRegexIndexPlan(model, 128, 1, 128));
    EXPECT_TRUE(ShouldPrepareRegexIndexPlan(model, model.rows, 8192, 8192));
    // Deferral in one batch must not prevent preparation in the next.
    EXPECT_TRUE(
        ShouldPrepareRegexIndexPlan(model, model.rows - 128, 8192, 8192));
}

TEST(RegexIndexPlan, NoConditionIsNeverAnEmptyResult) {
    size_t calls = 0;
    const auto plan = PrepareRegexIndexPlan(
        Condition{},
        [&](const auto&) {
            ++calls;
            return 0;
        },
        LargeSegment());
    EXPECT_EQ(calls, 0);
    EXPECT_FALSE(plan.Usable());
    EXPECT_FALSE(plan.empty_result);
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, 1000000, 8192, 8192));
    EXPECT_TRUE(std::isinf(RegexIndexExecutionCost(plan, 1000000, 8192, 8192)));
}

TEST(RegexIndexPlan, OnlyExactZeroCountsProveEmpty) {
    auto model = LargeSegment();
    model.cost_ratio = 0;
    const auto zero_and = Condition::Combine(
        Condition::Op::And, Literal("missing"), Literal("present"));
    const auto count = [](const std::string& literal) {
        return literal == "missing" ? 0 : 100;
    };
    const auto empty = PrepareRegexIndexPlan(zero_and, count, model);
    EXPECT_TRUE(empty.Usable());
    EXPECT_TRUE(empty.empty_result);
    // Empty proof survives a disabled or invalid heuristic cost ratio.
    EXPECT_TRUE(ShouldUseRegexIndexPlan(empty, model.rows, 1, 8192));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(empty, model.rows, 0, 8192));
    EXPECT_EQ(RegexIndexFixedCost(empty), 0);
    EXPECT_TRUE(CanRegexIndexPlanEverHelp(empty, model.rows, 8192));

    const auto nonzero_or = Condition::Combine(
        Condition::Op::Or, Literal("missing"), Literal("present"));
    const auto nonempty = PrepareRegexIndexPlan(nonzero_or, count, model);
    EXPECT_FALSE(nonempty.empty_result);
    EXPECT_EQ(nonempty.occurrences, 100);
    EXPECT_FALSE(ShouldUseRegexIndexPlan(nonempty, model.rows, 8192, 8192));

    const auto all_zero =
        PrepareRegexIndexPlan(nonzero_or, [](const auto&) { return 0; }, model);
    EXPECT_TRUE(all_zero.empty_result);
    EXPECT_TRUE(ShouldUseRegexIndexPlan(all_zero, model.rows, 1, 8192));
}

TEST(RegexIndexPlan, OccurrencesAreClampedToRowsForCandidateEstimation) {
    auto model = LargeSegment();
    model.rows = 1000;
    const auto plan = PrepareRegexIndexPlan(
        Literal("repeated"), [](const auto&) { return 5000; }, model);
    EXPECT_EQ(plan.occurrences, 5000);
    EXPECT_EQ(plan.estimated_rows, 1000);
    // Candidate estimation cannot reduce full occurrence enumeration.
    EXPECT_GE(RegexIndexExecutionCost(plan, 1000, 1000, 1000),
              40000000 * regex_index_detail::kLocateWorkScale);
}

TEST(RegexIndexPlan, InvalidCostsAndLargeCountsSafelyDecline) {
    auto model = LargeSegment();
    model.cost_ratio = std::numeric_limits<double>::quiet_NaN();
    EXPECT_FALSE(ShouldPrepareRegexIndexPlan(model, model.rows, 8192, 8192));
    auto plan = PrepareRegexIndexPlan(
        Literal("foo"), [](const auto&) { return 1; }, model);
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, model.rows, 8192, 8192));

    model.cost_ratio = 0.001;
    model.scan_view_bytes = std::numeric_limits<double>::infinity();
    EXPECT_FALSE(ShouldPrepareRegexIndexPlan(model, model.rows, 8192, 8192));
    plan = PrepareRegexIndexPlan(
        Literal("foo"), [](const auto&) { return 1; }, model);
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, model.rows, 8192, 8192));

    model = LargeSegment();
    plan = PrepareRegexIndexPlan(
        Literal("foo"),
        [](const auto&) { return std::numeric_limits<size_t>::max(); },
        model);
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, model.rows, 8192, 8192));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, 0, 8192, 8192));
    EXPECT_FALSE(ShouldUseRegexIndexPlan(plan, model.rows, 8192, 0));
}

}  // namespace
}  // namespace milvus
