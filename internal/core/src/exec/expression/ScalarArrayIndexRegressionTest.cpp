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

// Regression tests for the array element MATCH_* feature (PR #50669),
// covering the INDEXED code paths that the pre-existing tests never exercised
// (they only ever hit brute force). Each test either
//   (a) builds a NESTED (INVERTED) array index and asserts the indexed result
//       equals the brute-force (no-index) result for the same predicate, or
//   (b) builds a LEGACY FLAT (non-nested) array index and asserts the required
//       brute-force fallback still yields the correct result.
//
// Findings guarded here (see PR review):
//   P1-(1) deadlock: element-level MATCH_* over a nested array index must
//          COMPLETE (previously recursed into std::call_once and hung).
//   P1-(2): whole-array `arr == [...]` / `arr != [...]` over a NESTED array
//          index is now served EXACTLY by ExecArrayEqualForNestedIndex
//          (positional AND of per-value element bitsets); the differential
//          tests below pin it against the brute-force result, including
//          order sensitivity, duplicates, length mismatches, NULL rows,
//          VARCHAR elements and the empty target.
//   P2-(3): nullable array_contains* via a nested index must exclude NULL rows.
//   P2-(4): arithmetic element predicate over a FLAT array index must fall back.

#include <boost/container/vector.hpp>
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "common/Types.h"
#include "index/InvertedIndexTantivy.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

// Build an insert proto: pk (INT64) + a scalar Array<Int64> field `scores`.
// When `valid` is non-empty it is applied as the per-row null bitmap.
std::unique_ptr<InsertRecordProto>
BuildInt64ArrayInsert(const Schema& schema,
                      FieldId pk_fid,
                      FieldId scores_fid,
                      const std::vector<std::vector<int64_t>>& rows,
                      const std::vector<bool>& valid = {}) {
    auto insert_data = std::make_unique<InsertRecordProto>();
    const int64_t N = static_cast<int64_t>(rows.size());

    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array = CreateDataArrayFrom(ids.data(), nullptr, N, schema[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    std::vector<milvus::proto::schema::ScalarField> scores(N);
    for (int64_t i = 0; i < N; ++i) {
        for (auto v : rows[i]) {
            scores[i].mutable_long_data()->add_data(v);
        }
    }
    const bool* valid_ptr = nullptr;
    FixedVector<bool> valid_vec;
    if (!valid.empty()) {
        valid_vec.resize(N);
        for (int64_t i = 0; i < N; ++i) {
            valid_vec[i] = valid[i];
        }
        valid_ptr = valid_vec.data();
    }
    auto scores_array =
        CreateDataArrayFrom(scores.data(), valid_ptr, N, schema[scores_fid]);
    insert_data->mutable_fields_data()->AddAllocated(scores_array.release());

    insert_data->set_num_rows(N);
    return insert_data;
}

// Create a sealed segment with the insert proto's field data loaded.
std::shared_ptr<SegmentInterface>
MakeSealed(SchemaPtr schema, std::unique_ptr<InsertRecordProto> insert) {
    const int64_t N = insert->num_rows();
    GeneratedData generated;
    generated.schema_ = schema;
    generated.raw_ = insert.release();
    for (int64_t i = 0; i < N; ++i) {
        generated.row_ids_.push_back(i);
        generated.timestamps_.push_back(i);
    }
    auto seg = CreateSealedWithFieldDataLoaded(schema, generated);
    return std::shared_ptr<SegmentInterface>(std::move(seg));
}

// Build and load an INVERTED index on the int64 scalar-array field. `nested`
// selects a nested (per-element) index vs a legacy flat (per-row) index.
void
LoadInt64ArrayIndex(SegmentInterface* seg,
                    FieldId fid,
                    const std::vector<std::vector<int64_t>>& rows,
                    bool nested) {
    const size_t N = rows.size();
    std::vector<boost::container::vector<int64_t>> arrays(N);
    for (size_t i = 0; i < N; ++i) {
        for (auto v : rows[i]) {
            arrays[i].push_back(v);
        }
    }
    auto index = std::make_unique<index::InvertedIndexTantivy<int64_t>>();
    Config cfg;
    cfg["is_array"] = true;
    if (nested) {
        cfg["is_nested_index"] = true;
    }
    index->BuildWithRawDataForUT(N, arrays.data(), cfg);

    LoadIndexInfo info{};
    info.field_id = fid.get();
    info.index_params = GenIndexParams(index.get());
    info.cache_index = CreateTestCacheIndex("scores_idx", std::move(index));
    auto sealed = dynamic_cast<SegmentSealed*>(seg);
    ASSERT_NE(sealed, nullptr);
    sealed->LoadIndex(info);
}

// Parse `expr` against `schema`, run Retrieve, return matched segment offsets.
std::set<int64_t>
RetrieveRows(SegmentInterface* seg,
             const Schema& schema,
             SchemaPtr schema_ptr,
             const std::string& expr) {
    ScopedSchemaHandle schema_handle(schema);
    auto plan_str = schema_handle.Parse(expr);
    auto plan = milvus::query::CreateRetrievePlanByExpr(
        schema_ptr, plan_str.data(), plan_str.size());
    EXPECT_NE(plan, nullptr) << expr;
    auto result = seg->Retrieve(
        nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    EXPECT_NE(result, nullptr) << expr;
    std::set<int64_t> rows;
    for (const auto& offset : result->offset()) {
        rows.insert(offset);
    }
    return rows;
}

SchemaPtr
MakeInt64ArraySchema(FieldId& pk_fid, FieldId& scores_fid, bool nullable) {
    auto schema = std::make_shared<Schema>();
    pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, nullable);
    return schema;
}

// Build an insert proto: pk (INT64) + a scalar Array<VarChar> field `tags`.
std::unique_ptr<InsertRecordProto>
BuildVarcharArrayInsert(const Schema& schema,
                        FieldId pk_fid,
                        FieldId tags_fid,
                        const std::vector<std::vector<std::string>>& rows) {
    auto insert_data = std::make_unique<InsertRecordProto>();
    const int64_t N = static_cast<int64_t>(rows.size());

    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array = CreateDataArrayFrom(ids.data(), nullptr, N, schema[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    std::vector<milvus::proto::schema::ScalarField> tags(N);
    for (int64_t i = 0; i < N; ++i) {
        for (const auto& v : rows[i]) {
            tags[i].mutable_string_data()->add_data(v);
        }
    }
    auto tags_array =
        CreateDataArrayFrom(tags.data(), nullptr, N, schema[tags_fid]);
    insert_data->mutable_fields_data()->AddAllocated(tags_array.release());

    insert_data->set_num_rows(N);
    return insert_data;
}

// Build and load an INVERTED index on the varchar scalar-array field.
void
LoadVarcharArrayIndex(SegmentInterface* seg,
                      FieldId fid,
                      const std::vector<std::vector<std::string>>& rows,
                      bool nested) {
    const size_t N = rows.size();
    std::vector<boost::container::vector<std::string>> arrays(N);
    for (size_t i = 0; i < N; ++i) {
        for (const auto& v : rows[i]) {
            arrays[i].push_back(v);
        }
    }
    auto index = std::make_unique<index::InvertedIndexTantivy<std::string>>();
    Config cfg;
    cfg["is_array"] = true;
    if (nested) {
        cfg["is_nested_index"] = true;
    }
    index->BuildWithRawDataForUT(N, arrays.data(), cfg);

    LoadIndexInfo info{};
    info.field_id = fid.get();
    info.index_params = GenIndexParams(index.get());
    info.cache_index = CreateTestCacheIndex("tags_idx", std::move(index));
    auto sealed = dynamic_cast<SegmentSealed*>(seg);
    ASSERT_NE(sealed, nullptr);
    sealed->LoadIndex(info);
}

SchemaPtr
MakeVarcharArraySchema(FieldId& pk_fid, FieldId& tags_fid) {
    auto schema = std::make_shared<Schema>();
    pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    tags_fid =
        schema->AddDebugArrayField("tags", DataType::VARCHAR, /*nullable=*/
                                   false);
    return schema;
}

// The expr grammar rejects a literal `[]`, but a plan proto can legitimately
// carry an empty array target (hand-built plan / older SDK). Parse a
// single-element whole-array equality, rewrite the target to an EMPTY
// same_type array in the serialized plan, and Retrieve with the mutated plan.
std::set<int64_t>
RetrieveRowsEmptyArrayEquality(SegmentInterface* seg,
                               const Schema& schema,
                               SchemaPtr schema_ptr,
                               bool not_equal) {
    ScopedSchemaHandle schema_handle(schema);
    auto plan_str =
        schema_handle.Parse(not_equal ? "scores != [40]" : "scores == [40]");
    milvus::proto::plan::PlanNode plan_node;
    EXPECT_TRUE(plan_node.ParseFromArray(
        plan_str.data(), static_cast<int>(plan_str.size())));
    auto* pred = plan_node.has_query()
                     ? plan_node.mutable_query()->mutable_predicates()
                     : plan_node.mutable_predicates();
    // Locate the UnaryRangeExpr: emitted directly for both `==` and `!=`,
    // but tolerate a NOT-wrapped equality too.
    milvus::proto::plan::UnaryRangeExpr* unary = nullptr;
    if (pred->has_unary_range_expr()) {
        unary = pred->mutable_unary_range_expr();
    } else if (pred->has_unary_expr() &&
               pred->unary_expr().child().has_unary_range_expr()) {
        unary = pred->mutable_unary_expr()
                    ->mutable_child()
                    ->mutable_unary_range_expr();
    }
    EXPECT_NE(unary, nullptr);
    if (unary == nullptr) {
        return {};
    }
    auto* arr = unary->mutable_value()->mutable_array_val();
    arr->clear_array();
    arr->set_same_type(true);

    std::string mutated;
    EXPECT_TRUE(plan_node.SerializeToString(&mutated));
    auto plan = milvus::query::CreateRetrievePlanByExpr(
        schema_ptr, mutated.data(), mutated.size());
    EXPECT_NE(plan, nullptr);
    auto result = seg->Retrieve(
        nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    EXPECT_NE(result, nullptr);
    std::set<int64_t> rows;
    for (const auto& offset : result->offset()) {
        rows.insert(offset);
    }
    return rows;
}

}  // namespace

// ============================================================================
// P1-(1): element-level MATCH_* over a NESTED array index must COMPLETE and
// return the same rows as brute force. Without the DetermineExecPath fix (using
// the reentrant CanUseNestedIndex()) these recurse into std::call_once and hang,
// so merely COMPLETING is the regression signal. Covers the UnaryRange (`$ > k`,
// `$ == k`), BinaryRange (`a < $ < b`) and Term (`$ in [...]`) element paths.
TEST(ScalarArrayIndexRegression, IndexedElementMatchCompletesAndMatchesBrute) {
    FieldId pk_fid, scores_fid;
    auto schema = MakeInt64ArraySchema(pk_fid, scores_fid, /*nullable=*/false);

    std::vector<std::vector<int64_t>> rows = {
        {95, 80}, {40}, {100, 100, 100}, {}, {61, 75, 88}, {5, 200}};

    auto insert_i = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadInt64ArrayIndex(indexed.get(), scores_fid, rows, /*nested=*/true);

    auto insert_b = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "MATCH_ANY(scores, $ > 90)",          // UnaryRange element path
        "MATCH_ALL(scores, $ >= 60)",         // UnaryRange element path
        "MATCH_ANY(scores, $ == 100)",        // UnaryRange Equal element path
        "MATCH_ANY(scores, $ in [40, 200])",  // Term element path
        "MATCH_ANY(scores, 60 < $ < 90)",     // BinaryRange element path
        "MATCH_LEAST(scores, $ == 100, threshold=2)",
    };
    for (const auto& e : exprs) {
        // If P1-(1) regressed, the indexed Retrieve below never returns.
        auto idx_rows = RetrieveRows(indexed.get(), *schema, schema, e);
        auto brute_rows = RetrieveRows(brute.get(), *schema, schema, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "indexed vs brute-force divergence for: " << e;
    }
}

// P1-(2), now upgraded: whole-array equality `scores == [..]` over a NESTED
// array index is served EXACTLY by ExecArrayEqualForNestedIndex (positional
// AND of per-value element bitsets; nested postings are value->ELEMENT ids,
// so ExecArrayEqualForIndex's row-offset interpretation still must not run).
// Cross-check the nested-indexed segment against the no-index segment for
// Equal AND NotEqual, including order sensitivity and length mismatches.
TEST(ScalarArrayIndexRegression, WholeArrayEqualityWithNestedIndex) {
    FieldId pk_fid, scores_fid;
    auto schema = MakeInt64ArraySchema(pk_fid, scores_fid, /*nullable=*/false);

    std::vector<std::vector<int64_t>> rows = {
        {95, 80}, {40}, {100, 100, 100}, {}, {95, 80}, {80, 95}};

    auto insert_i = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadInt64ArrayIndex(indexed.get(), scores_fid, rows, /*nested=*/true);

    auto insert_b = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "scores == [95, 80]",  // matches rows {0, 4}; row5 [80,95] is ordered-different
        "scores != [95, 80]",
        "scores == [100, 100, 100]",
        "scores != [100, 100, 100]",
        "scores == [40]",
        "scores != [40]",
        "scores == [80, 95]",       // the reversed row must match ONLY here
        "scores == [95]",           // prefix of [95, 80]: length mismatch
        "scores != [95]",
        "scores == [95, 80, 100]",  // longer than every row
    };
    for (const auto& e : exprs) {
        auto idx_rows = RetrieveRows(indexed.get(), *schema, schema, e);
        auto brute_rows = RetrieveRows(brute.get(), *schema, schema, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "whole-array eq indexed vs brute divergence for: " << e;
    }
    // Anchor the brute-force truth so the cross-check can't pass by both paths
    // being wrong in the same way.
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores == [95, 80]"),
              (std::set<int64_t>{0, 4}));
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores != [95, 80]"),
              (std::set<int64_t>{1, 2, 3, 5}));
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores == [80, 95]"),
              (std::set<int64_t>{5}));
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores == [95]"),
              (std::set<int64_t>{}));
    EXPECT_EQ(
        RetrieveRows(brute.get(), *schema, schema, "scores == [95, 80, 100]"),
        (std::set<int64_t>{}));
}

// Duplicates in the target: each position must test its OWN value's bitset
// ([1,1,2] must not match the multiset-equal [1,2,1] / [2,1,1]), and the
// dedup of In() lookups must not collapse positions.
TEST(ScalarArrayIndexRegression, WholeArrayEqualityDuplicatesWithNestedIndex) {
    FieldId pk_fid, scores_fid;
    auto schema = MakeInt64ArraySchema(pk_fid, scores_fid, /*nullable=*/false);

    std::vector<std::vector<int64_t>> rows = {
        {1, 1, 2}, {1, 2, 1}, {2, 1, 1}, {1, 1}, {1, 1, 2, 2}, {2, 2, 1}};

    auto insert_i = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadInt64ArrayIndex(indexed.get(), scores_fid, rows, /*nested=*/true);

    auto insert_b = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "scores == [1, 1, 2]",
        "scores != [1, 1, 2]",
        "scores == [1, 1]",
        "scores != [1, 1]",
        "scores == [2, 2, 1]",
        "scores == [1, 2, 2]",  // matches no row
        "scores == [1, 1, 2, 2]",
    };
    for (const auto& e : exprs) {
        auto idx_rows = RetrieveRows(indexed.get(), *schema, schema, e);
        auto brute_rows = RetrieveRows(brute.get(), *schema, schema, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "duplicate-target eq indexed vs brute divergence for: " << e;
    }
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores == [1, 1, 2]"),
              (std::set<int64_t>{0}));
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores != [1, 1, 2]"),
              (std::set<int64_t>{1, 2, 3, 4, 5}));
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores == [1, 2, 2]"),
              (std::set<int64_t>{}));
}

// NULL rows under whole-array Equal/NotEqual via a nested index: a NULL row
// has zero elements in the index (indistinguishable from an empty array at
// element level), so the row-valid bitmap retained by ArrayOffsets must
// exclude it from BOTH `==` and `!=` (SQL 3VL), exactly like brute force.
TEST(ScalarArrayIndexRegression, WholeArrayEqualityNullableWithNestedIndex) {
    FieldId pk_fid, scores_fid;
    auto schema = MakeInt64ArraySchema(pk_fid, scores_fid, /*nullable=*/true);

    // row1 is NULL; row3 is an empty (but valid) array.
    std::vector<std::vector<int64_t>> rows = {
        {5, 8}, {}, {5, 9}, {}, {7}, {5, 8}};
    std::vector<bool> valid = {true, false, true, true, true, true};

    auto insert_i =
        BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows, valid);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    // Build the nested index from non-null rows only (null row -> empty slot).
    std::vector<std::vector<int64_t>> index_rows = rows;
    index_rows[1].clear();
    LoadInt64ArrayIndex(indexed.get(), scores_fid, index_rows, /*nested=*/true);

    auto insert_b =
        BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows, valid);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "scores == [5, 8]",
        "scores != [5, 8]",
        "scores == [7]",
        "scores != [7]",
    };
    for (const auto& e : exprs) {
        auto idx_rows = RetrieveRows(indexed.get(), *schema, schema, e);
        auto brute_rows = RetrieveRows(brute.get(), *schema, schema, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "nullable whole-array eq indexed vs brute divergence for: "
            << e;
    }
    // Anchor truth: Equal excludes the NULL row even though it has zero
    // elements; NotEqual ALSO excludes it (validity, not match), while the
    // empty-but-valid row 3 genuinely is "not equal".
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "scores == [5, 8]"),
              (std::set<int64_t>{0, 5}));
    auto neg = RetrieveRows(brute.get(), *schema, schema, "scores != [5, 8]");
    EXPECT_EQ(neg, (std::set<int64_t>{2, 3, 4}));
    EXPECT_EQ(neg.find(1), neg.end())
        << "NULL row must NOT satisfy `!=` (3VL validity, not match)";
    EXPECT_NE(neg.find(3), neg.end())
        << "empty (valid) row must satisfy `!=`";
}

// VARCHAR element type through the nested-index equality path (std::string /
// string_view index inner type + term lookups instead of numeric).
TEST(ScalarArrayIndexRegression, WholeArrayEqualityVarcharWithNestedIndex) {
    FieldId pk_fid, tags_fid;
    auto schema = MakeVarcharArraySchema(pk_fid, tags_fid);

    std::vector<std::vector<std::string>> rows = {
        {"a", "b"}, {"b", "a"}, {"a"}, {}, {"a", "b"}, {"a", "a"}};

    auto insert_i = BuildVarcharArrayInsert(*schema, pk_fid, tags_fid, rows);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadVarcharArrayIndex(indexed.get(), tags_fid, rows, /*nested=*/true);

    auto insert_b = BuildVarcharArrayInsert(*schema, pk_fid, tags_fid, rows);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "tags == ['a', 'b']",
        "tags != ['a', 'b']",
        "tags == ['b', 'a']",  // order matters
        "tags == ['a', 'a']",  // duplicate values
        "tags == ['a']",
        "tags != ['a']",
    };
    for (const auto& e : exprs) {
        auto idx_rows = RetrieveRows(indexed.get(), *schema, schema, e);
        auto brute_rows = RetrieveRows(brute.get(), *schema, schema, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "varchar whole-array eq indexed vs brute divergence for: "
            << e;
    }
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "tags == ['a', 'b']"),
              (std::set<int64_t>{0, 4}));
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "tags == ['a', 'a']"),
              (std::set<int64_t>{5}));
    EXPECT_EQ(RetrieveRows(brute.get(), *schema, schema, "tags == ['a']"),
              (std::set<int64_t>{2}));
}

// Empty target `arr == []`: the grammar rejects the literal, but a plan
// proto can carry it. With same_type set it matches exactly the rows whose
// array is empty AND non-NULL; `!=` matches the non-empty non-NULL rows.
TEST(ScalarArrayIndexRegression, WholeArrayEqualityEmptyTargetWithNestedIndex) {
    FieldId pk_fid, scores_fid;
    auto schema = MakeInt64ArraySchema(pk_fid, scores_fid, /*nullable=*/false);

    std::vector<std::vector<int64_t>> rows = {{95, 80}, {40}, {}, {7}};

    auto insert_i = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    LoadInt64ArrayIndex(indexed.get(), scores_fid, rows, /*nested=*/true);

    auto insert_b = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto brute = MakeSealed(schema, std::move(insert_b));

    for (bool not_equal : {false, true}) {
        auto idx_rows = RetrieveRowsEmptyArrayEquality(
            indexed.get(), *schema, schema, not_equal);
        auto brute_rows = RetrieveRowsEmptyArrayEquality(
            brute.get(), *schema, schema, not_equal);
        EXPECT_EQ(idx_rows, brute_rows)
            << "empty-target eq indexed vs brute divergence, not_equal="
            << not_equal;
    }
    EXPECT_EQ(RetrieveRowsEmptyArrayEquality(
                  brute.get(), *schema, schema, /*not_equal=*/false),
              (std::set<int64_t>{2}));
    EXPECT_EQ(RetrieveRowsEmptyArrayEquality(
                  brute.get(), *schema, schema, /*not_equal=*/true),
              (std::set<int64_t>{0, 1, 3}));
}

// P2-(4): an arithmetic element predicate (`$ % 2 == 0`) can only use a nested
// index; over a LEGACY FLAT (non-nested) array index it must fall back to brute
// force (rolling-upgrade correctness). Cross-check flat-indexed vs no-index.
TEST(ScalarArrayIndexRegression, ArithElementPredicateFallsBackOnFlatIndex) {
    FieldId pk_fid, scores_fid;
    auto schema = MakeInt64ArraySchema(pk_fid, scores_fid, /*nullable=*/false);

    std::vector<std::vector<int64_t>> rows = {
        {95, 80}, {41}, {100, 101, 102}, {}, {7}, {2, 4, 6}};

    auto insert_i = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto flat_indexed = MakeSealed(schema, std::move(insert_i));
    LoadInt64ArrayIndex(flat_indexed.get(), scores_fid, rows, /*nested=*/false);

    auto insert_b = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "MATCH_ANY(scores, $ % 2 == 0)",
        "MATCH_ALL(scores, $ % 2 == 0)",
    };
    for (const auto& e : exprs) {
        auto idx_rows = RetrieveRows(flat_indexed.get(), *schema, schema, e);
        auto brute_rows = RetrieveRows(brute.get(), *schema, schema, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "arith flat-index vs brute divergence for: " << e;
    }
    // Anchor truth: rows with any even element -> {0(80),2(100/102),5(all even)}.
    EXPECT_EQ(
        RetrieveRows(
            brute.get(), *schema, schema, "MATCH_ANY(scores, $ % 2 == 0)"),
        (std::set<int64_t>{0, 2, 5}));
}

// P2-(3): nullable array_contains* via a NESTED index must treat NULL rows the
// same as brute force. A NULL row has zero elements in the nested index, so
// without applying the raw field's per-row validity, `not array_contains(...)`
// wrongly INCLUDES NULL rows. Cross-check nested-indexed vs no-index.
TEST(ScalarArrayIndexRegression, NullableArrayContainsViaNestedIndex) {
    FieldId pk_fid, scores_fid;
    auto schema = MakeInt64ArraySchema(pk_fid, scores_fid, /*nullable=*/true);

    // row1 is NULL; row3 is an empty (but valid) array. Both have zero elements.
    std::vector<std::vector<int64_t>> rows = {{5, 8}, {}, {5, 9}, {}, {7}, {5}};
    std::vector<bool> valid = {true, false, true, true, true, true};

    auto insert_i =
        BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows, valid);
    auto indexed = MakeSealed(schema, std::move(insert_i));
    // Build the nested index from non-null rows only (null row -> empty slot).
    std::vector<std::vector<int64_t>> index_rows = rows;
    index_rows[1].clear();
    LoadInt64ArrayIndex(indexed.get(), scores_fid, index_rows, /*nested=*/true);

    auto insert_b =
        BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows, valid);
    auto brute = MakeSealed(schema, std::move(insert_b));

    const std::vector<std::string> exprs = {
        "array_contains(scores, 5)",
        "not array_contains(scores, 5)",
        "array_contains_any(scores, [5, 7])",
        "not array_contains_any(scores, [5, 7])",
    };
    for (const auto& e : exprs) {
        auto idx_rows = RetrieveRows(indexed.get(), *schema, schema, e);
        auto brute_rows = RetrieveRows(brute.get(), *schema, schema, e);
        EXPECT_EQ(idx_rows, brute_rows)
            << "nullable array_contains index vs brute divergence for: " << e;
    }
    // Anchor truth: NULL row (1) is excluded from BOTH array_contains and its
    // negation (SQL NULL semantics), exactly as the raw-data path does.
    auto neg = RetrieveRows(
        brute.get(), *schema, schema, "not array_contains(scores, 5)");
    EXPECT_EQ(neg.find(1), neg.end())
        << "NULL row must NOT satisfy `not array_contains` (validity, not "
           "match)";
    // Empty (valid) row 3 IS in the negation (it genuinely does not contain 5).
    EXPECT_NE(neg.find(3), neg.end())
        << "empty (valid) row must satisfy `not array_contains`";
}
