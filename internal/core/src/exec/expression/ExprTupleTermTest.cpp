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

// Tests for PhyTupleTermFilterExpr / TupleTermFilterExpr, the correlated
// multi-column exact membership predicate `[a, b] in [[v1,w1], ...]`. See
// design doc docs/design-docs/design_docs/20260901-tuple-term-membership-expression.md.
//
// These tests build expr::TupleTermFilterExpr directly (bypassing plan-proto
// string parsing entirely, the same way ExprCompareTest.cpp's
// test_term_pk_with_sorted builds expr::TermFilterExpr directly) so they
// exercise exactly the C++ execution logic in TupleTermExpr.cpp, independent
// of the Go parser side.
//
// IMPORTANT: this file could not be compiled or run in the environment it
// was authored in (no local C++ build was available -- see the design doc's
// note on verification). It is written to compile and pass by careful
// reading of the precedent files (ExprCompareTest.cpp, ExprTermTest.cpp,
// TupleTermExpr.cpp itself) but has NOT been confirmed to do so; treat CI's
// first run of it as the real verification.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ExprTestBase.h"
#include "NamedType/named_type_impl.hpp"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/TupleMembership.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

EXPR_TEST_INSTANTIATE();

namespace {

// Builds a TupleMembership directly from already-encoded keys, using exactly
// the shared encode functions PhyTupleTermFilterExpr and
// ProtoParser::ParseTupleTermFilterExprs both call -- so a membership set
// built this way in a test is equivalent to one built by the real parser
// from an equivalent plan proto.
std::shared_ptr<const milvus::TupleMembership>
MakeInt64PairMembership(
    const std::vector<std::pair<int64_t, int64_t>>& tuples) {
    std::unordered_set<std::string> keys;
    for (auto& [a, b] : tuples) {
        std::string key;
        milvus::EncodeTupleElementInt64(a, key);
        milvus::EncodeTupleElementInt64(b, key);
        keys.insert(std::move(key));
    }
    return std::make_shared<const milvus::TupleMembership>(std::move(keys));
}

}  // namespace

// The defining behavior this feature exists for: a two-column tuple filter
// must match exact CORRELATED pairs, not the independent per-column
// Cartesian product an AND of two single-column IN predicates would compute
// (see design doc Motivation). Builds the membership set from two rows'
// REAL generated (a, b) pairs so the test is independent of DataGen's exact
// generation formula, then verifies: both real pairs match; a "crossed"
// pair built from one row's `a` and the other row's `b` does NOT match
// (would incorrectly match under Cartesian-product semantics); every other
// row does not match.
TEST_P(ExprTest, TestTupleTermCorrelatedNotCartesian) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto a_fid = schema->AddDebugField("col_a", DataType::INT64);
    auto b_fid = schema->AddDebugField("col_b", DataType::INT64);
    schema->set_primary_field_id(a_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int64_t N = 1000;
    auto raw_data = DataGen(schema, N);
    auto col_a = raw_data.get_col<int64_t>(a_fid);
    auto col_b = raw_data.get_col<int64_t>(b_fid);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Find two rows whose (a, b) pairs are pairwise distinct on both
    // columns, so a crossed combination cannot coincidentally equal a real
    // row's pair regardless of DataGen's exact value distribution.
    int64_t row1 = -1, row2 = -1;
    for (int64_t i = 0; i < N && row2 < 0; ++i) {
        for (int64_t j = i + 1; j < N; ++j) {
            if (col_a[i] != col_a[j] && col_b[i] != col_b[j]) {
                row1 = i;
                row2 = j;
                break;
            }
        }
    }
    ASSERT_GE(row1, 0) << "could not find two rows with distinct a and b";
    ASSERT_GE(row2, 0);

    auto membership = MakeInt64PairMembership(
        {{col_a[row1], col_b[row1]}, {col_a[row2], col_b[row2]}});
    auto expr = std::make_shared<expr::TupleTermFilterExpr>(
        std::vector<expr::ColumnInfo>{expr::ColumnInfo(a_fid, DataType::INT64),
                                      expr::ColumnInfo(b_fid, DataType::INT64)},
        membership);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final_bits = ExecuteQueryExpr(plan, seg_promote, N, MAX_TIMESTAMP);
    ASSERT_EQ(final_bits.size(), N);

    // The membership set contains only the two REAL (a, b) pairs from row1
    // and row2 -- never the crossed combinations (col_a[row1], col_b[row2])
    // or (col_a[row2], col_b[row1]), which a Cartesian-product
    // implementation (independent per-column IN, ANDed) would wrongly
    // treat as matches. Every row matches iff its own (a, b) equals one of
    // the two real pairs exactly -- a coincidental duplicate of a target
    // pair elsewhere in the data is a legitimate match, not just row1/row2.
    for (int64_t i = 0; i < N; ++i) {
        bool is_real_pair =
            (col_a[i] == col_a[row1] && col_b[i] == col_b[row1]) ||
            (col_a[i] == col_a[row2] && col_b[i] == col_b[row2]);
        EXPECT_EQ(final_bits[i], is_real_pair) << "row " << i;
    }
    EXPECT_TRUE(final_bits[row1]);
    EXPECT_TRUE(final_bits[row2]);

    // Sanity-check the crossed combination actually differs from both real
    // pairs, i.e. this row selection genuinely exercises the Cartesian trap
    // rather than degenerating to a single distinct pair.
    EXPECT_NE(col_a[row1], col_a[row2]);
    EXPECT_NE(col_b[row1], col_b[row2]);
}

// Three columns of mixed types (INT64, VARCHAR, BOOL), sealed segment.
TEST_P(ExprTest, TestTupleTermThreeColumnsSealed) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto a_fid = schema->AddDebugField("col_a", DataType::INT64);
    auto b_fid = schema->AddDebugField("col_b", DataType::VARCHAR);
    auto c_fid = schema->AddDebugField("col_c", DataType::BOOL);
    schema->set_primary_field_id(a_fid);

    auto seg = CreateSealedSegment(schema);
    int64_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    auto col_a = raw_data.get_col<int64_t>(a_fid);
    auto col_b = raw_data.get_col<std::string>(b_fid);
    auto col_c = raw_data.get_col<bool>(c_fid);

    int64_t target_row = 3;
    std::unordered_set<std::string> keys;
    {
        std::string key;
        milvus::EncodeTupleElementInt64(col_a[target_row], key);
        milvus::EncodeTupleElementBytes(
            col_b[target_row].data(), col_b[target_row].size(), key);
        milvus::EncodeTupleElementBool(col_c[target_row], key);
        keys.insert(std::move(key));
    }
    auto membership =
        std::make_shared<const milvus::TupleMembership>(std::move(keys));

    auto expr = std::make_shared<expr::TupleTermFilterExpr>(
        std::vector<expr::ColumnInfo>{
            expr::ColumnInfo(a_fid, DataType::INT64),
            expr::ColumnInfo(b_fid, DataType::VARCHAR),
            expr::ColumnInfo(c_fid, DataType::BOOL)},
        membership);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final_bits = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final_bits.size(), N);

    for (int64_t i = 0; i < N; ++i) {
        bool is_target = (col_a[i] == col_a[target_row]) &&
                         (col_b[i] == col_b[target_row]) &&
                         (col_c[i] == col_c[target_row]);
        EXPECT_EQ(final_bits[i], is_target) << "row " << i;
    }
    EXPECT_TRUE(final_bits[target_row]);
}

// A row where the tuple's column is NULL must never match, under the plain
// predicate or its NOT wrapper -- NOT is the framework's existing generic
// LogicalUnaryExpr/PhyLogicalUnaryExpr, not a bespoke polarity on
// TupleTermFilterExpr itself (see design doc), so this also exercises that
// NULL propagates through negation correctly rather than being flipped to a
// false positive by naive boolean negation.
TEST_P(ExprTest, TestTupleTermNullColumnExcludedBothPolarities) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto a_fid = schema->AddDebugField("col_a", DataType::INT64);
    auto b_fid =
        schema->AddDebugField("col_b", DataType::INT64, /*nullable=*/true);
    schema->set_primary_field_id(a_fid);

    auto seg = CreateSealedSegment(schema);
    int64_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    auto col_a = raw_data.get_col<int64_t>(a_fid);
    auto col_b = raw_data.get_col<int64_t>(b_fid);
    auto valid_b = raw_data.get_col_valid(b_fid);

    int64_t null_row = -1;
    for (int64_t i = 0; i < N; ++i) {
        if (!valid_b[i]) {
            null_row = i;
            break;
        }
    }
    ASSERT_GE(null_row, 0)
        << "DataGen did not produce any NULL row for the nullable column; "
           "test cannot exercise NULL exclusion without one";

    // Deliberately include the NULL row's (a, b) as if it were a target: if
    // NULL exclusion is broken, this is the pairing most likely to
    // accidentally "match" (e.g. via an unencoded/zero-valued NULL slot
    // aliasing a real value), so target it directly rather than an
    // unrelated pair.
    auto membership =
        MakeInt64PairMembership({{col_a[null_row], col_b[null_row]}});

    auto expr = std::make_shared<expr::TupleTermFilterExpr>(
        std::vector<expr::ColumnInfo>{expr::ColumnInfo(a_fid, DataType::INT64),
                                      expr::ColumnInfo(b_fid, DataType::INT64)},
        membership);

    // Positive polarity.
    {
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto final_bits = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final_bits.size(), N);
        EXPECT_FALSE(final_bits[null_row]);
    }

    // Negated: [...] not in [...]. Must NOT "recover" the NULL row -- a
    // naive boolean negation of a false result would wrongly include it.
    {
        auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, expr);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           not_expr);
        auto final_bits = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final_bits.size(), N);
        EXPECT_FALSE(final_bits[null_row])
            << "NOT must not recover a row excluded because a "
               "participating column is NULL";
    }
}

// An empty tuple set matches nothing, mirroring `field in []` for
// single-column IN.
TEST_P(ExprTest, TestTupleTermEmptySetMatchesNothing) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto a_fid = schema->AddDebugField("col_a", DataType::INT64);
    auto b_fid = schema->AddDebugField("col_b", DataType::INT64);
    schema->set_primary_field_id(a_fid);

    auto seg = CreateSealedSegment(schema);
    int64_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    auto membership = std::make_shared<const milvus::TupleMembership>(
        std::unordered_set<std::string>{});
    auto expr = std::make_shared<expr::TupleTermFilterExpr>(
        std::vector<expr::ColumnInfo>{expr::ColumnInfo(a_fid, DataType::INT64),
                                      expr::ColumnInfo(b_fid, DataType::INT64)},
        membership);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final_bits = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final_bits.size(), N);
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_FALSE(final_bits[i]) << "row " << i;
    }
}

// Growing segment without raw data ever missing is implicitly covered by
// TestTupleTermCorrelatedNotCartesian above (it uses CreateGrowingSegment).
// A field genuinely missing raw data (the v1 fail-closed case) is exercised
// directly here: constructing the physical expression must throw rather
// than silently degrade to an unfiltered scan.
TEST_P(ExprTest, TestTupleTermMissingRawDataFailsClosed) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto a_fid = schema->AddDebugField("col_a", DataType::INT64);
    auto b_fid = schema->AddDebugField("col_b", DataType::INT64);
    schema->set_primary_field_id(a_fid);

    // A freshly-constructed sealed segment with no data loaded at all has
    // no raw field data for either column.
    auto seg = CreateSealedSegment(schema);

    auto membership = MakeInt64PairMembership({{1, 2}});
    auto expr = std::make_shared<expr::TupleTermFilterExpr>(
        std::vector<expr::ColumnInfo>{expr::ColumnInfo(a_fid, DataType::INT64),
                                      expr::ColumnInfo(b_fid, DataType::INT64)},
        membership);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    EXPECT_ANY_THROW({ ExecuteQueryExpr(plan, seg.get(), 0, MAX_TIMESTAMP); });
}
