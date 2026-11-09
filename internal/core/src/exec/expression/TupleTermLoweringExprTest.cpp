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

// Tests the NULL semantics of the LOWERED form correlated multi-column
// `[a, b] in [[v1,w1], ...]` emits -- OR of ANDs of column==literal, see
// buildTupleTermExpr in internal/parser/planparserv2/tuple_term_filter.go
// and docs/design-docs/design_docs/20261012-tuple-term-membership-lowering.md
// -- end to end, against a real nullable growing segment, rather than by
// reasoning about ConjunctExpr.cpp's truth table by hand.
//
// This file exists because of a PR review finding: the design doc argued
// "no C++ test needed -- the lowered form reuses already-tested primitives
// (AND/OR/UnaryRangeExpr)". That is true of each primitive in isolation, but
// the specific COMPOSITION this feature introduces -- an OR over several
// multi-column AND branches, each potentially touching a NULL column in a
// different tuple -- had never actually been exercised end to end before
// this file. Builds the exact IR tree the Go lowering emits directly
// (expr::UnaryRangeFilterExpr + expr::LogicalBinaryExpr), bypassing the Go
// parser and plan-proto text entirely, the same way ExprCompareTest.cpp and
// ExprTermTest.cpp in this directory build IR directly to isolate C++
// execution from parsing.
//
// IMPORTANT: this file could not be compiled or run in the environment it
// was authored in (no local C++ build available). It is written to compile
// and pass by careful reading of sibling precedent in this directory
// (ExprTermTest.cpp, ExprCompareTest.cpp, ConjunctExprTest.cpp,
// test_exec.cpp's TaskTest.LogicalExpr) and of test_bloom_filter_expr.cpp's
// OverwriteJsonColumn pattern for injecting exact values into a DataGen-
// generated dataset, but has not been confirmed to do so; CI's first run of
// it is the real verification.

#include <folly/FBVector.h>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "ExprTestBase.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

EXPR_TEST_INSTANTIATE();

namespace {

proto::plan::GenericValue
Int64Value(int64_t v) {
    proto::plan::GenericValue value;
    value.set_int64_val(v);
    return value;
}

// Builds (col_a == v1 AND col_b == w1) OR (col_a == v2 AND col_b == w2) OR
// ..., exactly the tree buildTupleTermExpr emits for
// `[col_a, col_b] in [[v1,w1], [v2,w2], ...]`.
expr::TypedExprPtr
BuildLoweredOrOfAnds(FieldId col_a_fid,
                     FieldId col_b_fid,
                     const std::vector<std::pair<int64_t, int64_t>>& tuples) {
    std::vector<expr::TypedExprPtr> branches;
    for (const auto& [v, w] : tuples) {
        auto eq_a = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(col_a_fid, DataType::INT64, {}, true),
            proto::plan::OpType::Equal,
            Int64Value(v));
        auto eq_b = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(col_b_fid, DataType::INT64, {}, true),
            proto::plan::OpType::Equal,
            Int64Value(w));
        branches.push_back(std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, eq_a, eq_b));
    }
    expr::TypedExprPtr tree = branches[0];
    for (size_t i = 1; i < branches.size(); ++i) {
        tree = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, tree, branches[i]);
    }
    return tree;
}

// A row's three-valued verdict, derived from running BOTH polarities (the
// plain lowered tree, and NOT of it) rather than assumed from one bitmap
// alone: ExecuteQueryExpr returns a plain BitsetType that already collapses
// UNKNOWN to "excluded" for whichever single polarity it ran, so a definite
// FALSE and a genuine UNKNOWN look identical in one bitmap -- distinguishing
// them is the entire point of this file. NOT's own UNKNOWN-preserving
// behavior is pre-existing, separately tested machinery (LogicalUnaryExpr;
// see ConjunctExprTest.cpp's MarkNullRejectingStopsAtNonConjunctNodes), not
// re-derived here.
enum class RowVerdict { kTrue, kFalse, kUnknown };

RowVerdict
Classify(bool in_bit, bool not_in_bit) {
    if (in_bit) {
        return RowVerdict::kTrue;
    }
    if (not_in_bit) {
        return RowVerdict::kFalse;
    }
    return RowVerdict::kUnknown;
}

}  // namespace

TEST_P(ExprTest, TupleLoweringNullSemanticsEndToEnd) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto a_fid = schema->AddDebugField("col_a", DataType::INT64, true);
    auto b_fid = schema->AddDebugField("col_b", DataType::INT64, true);
    schema->set_primary_field_id(pk_fid);

    constexpr int64_t N = 6;
    auto raw_data = DataGen(schema, N);

    // Overwrite col_a/col_b with exact, hand-picked values. DataGen's random
    // generation cannot reliably place a NULL in a specific row alongside a
    // specific small integer in the other column, which these cases need
    // (e.g. row 3 needs col_a NULL and col_b == 3, exactly) -- the same
    // technique test_bloom_filter_expr.cpp's OverwriteJsonColumn uses, here
    // for INT64 (long_data) instead of JSON.
    struct Row {
        int64_t a;
        bool valid_a;
        int64_t b;
        bool valid_b;
    };
    const std::vector<Row> rows = {
        {1, true, 1, true},   // row 0: exact match of tuple (1,1)
        {2, true, 2, true},   // row 1: exact match of tuple (2,2)
        {0, false, 1, true},  // row 2: (NULL,1) -- genuinely ambiguous
        {0, false, 3, true},  // row 3: (NULL,3) -- definite non-match
        {1, true, 0, false},  // row 4: (1,NULL) -- ambiguous, swapped column
        {3, true, 0, false},  // row 5: (3,NULL) -- definite, swapped column
    };
    ASSERT_EQ(static_cast<int64_t>(rows.size()), N);

    auto overwrite_int64_column = [&](FieldId fid,
                                      const std::vector<int64_t>& values,
                                      const std::vector<bool>& valid) {
        bool found = false;
        for (auto& fd : *raw_data.raw_->mutable_fields_data()) {
            if (fd.field_id() != fid.get()) {
                continue;
            }
            found = true;
            auto* data =
                fd.mutable_scalars()->mutable_long_data()->mutable_data();
            data->Clear();
            for (auto v : values) {
                data->Add(v);
            }
            auto* valid_data = fd.mutable_scalars()->mutable_valid_data();
            valid_data->Clear();
            for (bool v : valid) {
                valid_data->Add(v);
            }
        }
        ASSERT_TRUE(found) << "field " << fid.get()
                           << " not present in generated dataset";
    };

    std::vector<int64_t> a_values, b_values;
    std::vector<bool> a_valid, b_valid;
    for (const auto& r : rows) {
        a_values.push_back(r.a);
        a_valid.push_back(r.valid_a);
        b_values.push_back(r.b);
        b_valid.push_back(r.valid_b);
    }
    overwrite_int64_column(a_fid, a_values, a_valid);
    overwrite_int64_column(b_fid, b_values, b_valid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.SharedRaw());
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    auto run = [&](const expr::TypedExprPtr& e) {
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, e);
        auto bits = ExecuteQueryExpr(plan, seg_promote, N, MAX_TIMESTAMP);
        EXPECT_EQ(static_cast<int64_t>(bits.size()), N);
        return bits;
    };

    {
        SCOPED_TRACE("two tuples: [[1,1],[2,2]]");
        auto top = BuildLoweredOrOfAnds(a_fid, b_fid, {{1, 1}, {2, 2}});
        auto not_top = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, top);
        auto in_bits = run(top);
        auto not_in_bits = run(not_top);

        EXPECT_EQ(Classify(in_bits[0], not_in_bits[0]), RowVerdict::kTrue);
        EXPECT_EQ(Classify(in_bits[1], not_in_bits[1]), RowVerdict::kTrue);
        EXPECT_EQ(Classify(in_bits[2], not_in_bits[2]), RowVerdict::kUnknown)
            << "(NULL,1) must be UNKNOWN: col_b alone doesn't rule out "
               "tuple (1,1)";
        EXPECT_EQ(Classify(in_bits[3], not_in_bits[3]), RowVerdict::kFalse)
            << "(NULL,3) must be definite FALSE: col_b==3 rules out both "
               "tuples regardless of col_a";
        EXPECT_EQ(Classify(in_bits[4], not_in_bits[4]), RowVerdict::kUnknown)
            << "(1,NULL) must be UNKNOWN (swapped-column analogue of row 2)";
        EXPECT_EQ(Classify(in_bits[5], not_in_bits[5]), RowVerdict::kFalse)
            << "(3,NULL) must be definite FALSE (swapped-column analogue of "
               "row 3)";
    }

    {
        SCOPED_TRACE(
            "three tuples: [[1,1],[2,2],[3,3]] -- adding a tuple can turn a "
            "definite FALSE into UNKNOWN");
        auto top = BuildLoweredOrOfAnds(a_fid, b_fid, {{1, 1}, {2, 2}, {3, 3}});
        auto not_top = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, top);
        auto in_bits = run(top);
        auto not_in_bits = run(not_top);

        // Row 5 (3,NULL) was a definite non-match against only
        // {(1,1),(2,2)} because col_a==3 didn't match either target's first
        // value. Adding tuple (3,3) changes that: col_a==3 now matches a
        // tuple's first value, so the match now genuinely depends on the
        // NULL col_b -- the OR must correctly carry that ambiguity across
        // three branches, not just two.
        EXPECT_EQ(Classify(in_bits[5], not_in_bits[5]), RowVerdict::kUnknown)
            << "(3,NULL) must become UNKNOWN once a (3,*) tuple is added";
        // Rows 0/1 are still definite matches; the added third branch must
        // not disturb them.
        EXPECT_EQ(Classify(in_bits[0], not_in_bits[0]), RowVerdict::kTrue);
        EXPECT_EQ(Classify(in_bits[1], not_in_bits[1]), RowVerdict::kTrue);
    }
}
