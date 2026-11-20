// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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

#include <chrono>
#include <fstream>
#include <thread>
#include "query/ExecPlanNodeVisitor.h"
#include "exec/expression/Expr.h"
#include "exec/expression/function/FunctionFactory.h"
#include "segcore/segment_c.h"
#include "segcore/UpdatesRecord.h"
#include "segcore/test_utils/TextMatchTestUtils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::segcore::text_test;

TEST(SegmentTextMatchTest, GrowingNaive) {
    auto schema = GenTestSchema();
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.owner);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
    }

    {
        // A typo (edit distance 1) matches the same rows as the exact term and
        // exercises the executor + growing commit/reload path for fuzzy match.
        BitsetType final;
        auto expr = GetFuzzyMatchExpr(schema, "footbal", 1);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "footbal", OpType::TextMatchFuzzy, 1);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
    }

    {
        // The executor rejects an out-of-range or missing max_edit_distance
        // (the parser guarantees [0, 2], but a raw proto may not).
        auto bad = GetFuzzyMatchExpr(schema, "footbal", 3);
        ASSERT_ANY_THROW(ExecuteQueryExpr(bad, seg.get(), N, MAX_TIMESTAMP));
        auto missing = GetFuzzyMatchExprNoDistance(schema, "footbal");
        ASSERT_ANY_THROW(
            ExecuteQueryExpr(missing, seg.get(), N, MAX_TIMESTAMP));
    }
}

// Regression test for https://github.com/milvus-io/milvus/issues/48388
// On growing segments, the TextIndex may index rows beyond the query timestamp.
// ExecTextMatch must truncate the result bitmap to active_count to avoid
// FilterBitsNode assertion failure: col_vec_size != need_process_rows_.
TEST(SegmentTextMatchTest, GrowingIndexAheadOfActiveCount) {
    auto schema = GenTestSchema();
    auto seg = CreateGrowingSegment(schema, empty_index_meta);

    // Batch 1: 3 rows with timestamps 0,1,2
    int64_t N1 = 3;
    uint64_t seed = 19190504;
    auto data1 = DataGen(schema, N1, seed);
    auto str_col1 = data1.raw_->mutable_fields_data()
                        ->at(1)
                        .mutable_scalars()
                        ->mutable_string_data()
                        ->mutable_data();
    str_col1->at(0) = "football, basketball";
    str_col1->at(1) = "swimming, tennis";
    str_col1->at(2) = "football, swimming";
    seg->PreInsert(N1);
    seg->Insert(
        0, N1, data1.row_ids_.data(), data1.timestamps_.data(), data1.owner);

    // Batch 2: 2 rows with timestamps 3,4 (later than batch 1)
    int64_t N2 = 2;
    auto data2 = DataGen(schema, N2, seed, N1);  // ts_offset=N1 → ts 3,4
    // Fix row_ids to continue from batch 1
    for (int i = 0; i < N2; i++) {
        data2.row_ids_[i] = N1 + i;
    }
    auto str_col2 = data2.raw_->mutable_fields_data()
                        ->at(1)
                        .mutable_scalars()
                        ->mutable_string_data()
                        ->mutable_data();
    str_col2->at(0) = "football, rugby";
    str_col2->at(1) = "football, cricket";
    seg->PreInsert(N2);
    seg->Insert(
        N1, N2, data2.row_ids_.data(), data2.timestamps_.data(), data2.owner);

    // Wait for TextIndex to index all 5 rows
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);

    // Query with timestamp=2 so only batch 1 (3 rows) is visible,
    // but TextIndex has indexed all 5 rows.
    // Before fix: assertion failure (bitset size 5 != need_process_rows 3)
    // After fix: result is correctly truncated to 3 rows
    int64_t query_ts = N1 - 1;  // timestamp=2, active_count should be N1
    for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
        auto expr = GetMatchExpr(schema, "football", op);
        BitsetType result = ExecuteQueryExpr(expr, seg.get(), N1, query_ts);
        ASSERT_EQ(result.size(), N1)
            << "result bitmap size should equal active_count (N1=" << N1
            << "), not total indexed rows (" << N1 + N2 << ")";
        // Row 0 ("football, basketball") and row 2 ("football, swimming") match
        ASSERT_TRUE(result[0]);
        ASSERT_FALSE(result[1]);
        ASSERT_TRUE(result[2]);
    }

    // Also verify full-range query still works correctly
    for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
        auto expr = GetMatchExpr(schema, "football", op);
        BitsetType result =
            ExecuteQueryExpr(expr, seg.get(), N1 + N2, MAX_TIMESTAMP);
        ASSERT_EQ(result.size(), N1 + N2);
        ASSERT_TRUE(result[0]);   // "football, basketball"
        ASSERT_FALSE(result[1]);  // "swimming, tennis"
        ASSERT_TRUE(result[2]);   // "football, swimming"
        ASSERT_TRUE(result[3]);   // "football, rugby"
        ASSERT_TRUE(result[4]);   // "football, cricket"
    }
}

TEST(SegmentTextMatchTest, UpdatedNonNullableGrowingTextWithUncoveredTail) {
    // Hold a real committed reader generation across the next insert so that
    // query-time autocommit cannot make the uncovered-tail case timing-dependent.
    class GrowingWithPinnedText final : public SegmentGrowingImpl {
     public:
        using SegmentGrowingImpl::SegmentGrowingImpl;
        milvus::index::GrowingIndexSnapshotPin prefix;
        milvus::index::GrowingIndexSnapshotPin
        PinGrowingIndex(milvus::OpContext* ctx, FieldId field) const override {
            return prefix && field == FieldId(101)
                       ? prefix
                       : SegmentGrowingImpl::PinGrowingIndex(ctx, field);
        }
    };

    auto schema = GenTestSchema();
    GrowingWithPinnedText segment(
        schema, empty_index_meta, SegcoreConfig::default_config(), 72001);
    auto prefix_data = DataGen(schema, 2);
    auto* strings = prefix_data.raw_->mutable_fields_data(1)
                        ->mutable_scalars()
                        ->mutable_string_data();
    strings->set_data(0, "swimming");
    strings->set_data(1, "football");
    auto info = PrepareInsertBinlog(
        1,
        2,
        3,
        prefix_data,
        storage::CreateChunkManager(get_default_local_storage_config()));
    const auto status = LoadFieldData(&segment, &info);
    ASSERT_EQ(status.error_code, Success);
    segment.prefix = segment.PinGrowingIndex(nullptr, FieldId(101));
    ASSERT_TRUE(segment.prefix);
    ASSERT_EQ(segment.prefix.CoveredRowEnd(), 2);
    auto tail = DataGen(schema, 1, 0, 2);
    tail.raw_->mutable_fields_data(1)
        ->mutable_scalars()
        ->mutable_string_data()
        ->set_data(0, "football");
    const auto offset = segment.PreInsert(1);
    segment.Insert(
        offset, 1, tail.row_ids_.data(), tail.timestamps_.data(), tail.owner);
    ASSERT_EQ(segment.get_active_count(20), 3);

    IdArray ids;
    ids.mutable_int_id()->add_data(0);
    UpdateAssignment a{FieldId(101), {}};
    a.literal.mutable_scalar_value()->set_string_data("football");
    segment.ApplyUpdates(ids, 20, 0, {a});

    auto logical =
        GetMatchExpr(schema, "football", OpType::TextMatch)->filter();
    for (auto ts : {19, 20}) {
        exec::QueryContext query_ctx(DEAFULT_QUERY_ID, &segment, 3, ts);
        exec::ExecContext exec_ctx(&query_ctx);
        exec::ExprSet expressions({logical}, &exec_ctx);
        exec::EvalCtx ctx(&exec_ctx);
        std::vector<VectorPtr> output;
        expressions.Eval(0, 1, true, ctx, output);
        auto column = std::static_pointer_cast<ColumnVector>(output.at(0));
        ASSERT_EQ(column->size(), 3);
        TargetBitmapView hits(column->GetRawData(), 3);
        TargetBitmapView valid(column->GetValidRawData(), 3);
        EXPECT_EQ(hits[0], ts >= 20);
        EXPECT_TRUE(hits[1]);
        EXPECT_FALSE(hits[2]);  // The pinned generation has no posting for it.
        EXPECT_TRUE(valid[0]);
        EXPECT_TRUE(valid[1]);
        EXPECT_TRUE(valid[2]);  // Its schema still guarantees a non-NULL row.
    }
}

TEST(SegmentTextMatchTest, GrowingNaiveNullable) {
    auto schema = GenTestSchema({}, true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {
        "football, basketball, pingpang", "swimming, football", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    auto str_col_valid = raw_data.raw_->mutable_fields_data()
                             ->at(1)
                             .mutable_scalars()
                             ->mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.owner);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(SegmentTextMatchTest, SealedNaive) {
    auto schema = GenTestSchema();
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
    }

    {
        // A typo (edit distance 1) matches the same rows as the exact term,
        // exercising the executor dispatch for fuzzy on a sealed segment.
        BitsetType final;
        auto expr = GetFuzzyMatchExpr(schema, "footbal", 1);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "footbal", OpType::TextMatchFuzzy, 1);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
    }
}

TEST(SegmentTextMatchTest, SealedNaiveNullable) {
    auto schema = GenTestSchema({}, true);
    std::vector<std::string> raw_str = {
        "football, basketball, pingpang", "swimming, football", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid = raw_data.raw_->mutable_fields_data()
                             ->at(1)
                             .mutable_scalars()
                             ->mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(SegmentTextMatchTest, GrowingJieBa) {
    auto schema = GenTestSchema({
        {"enable_match", "true"},
        {"enable_analyzer", "true"},
        {"analyzer_params", R"({"tokenizer": "jieba"})"},
    });
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.owner);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}

TEST(SegmentTextMatchTest, GrowingJieBaNullable) {
    auto schema = GenTestSchema(
        {
            {"enable_match", "true"},
            {"enable_tokenizer", "true"},
            {"analyzer_params", R"({"tokenizer": "jieba"})"},
        },
        true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid = raw_data.raw_->mutable_fields_data()
                             ->at(1)
                             .mutable_scalars()
                             ->mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.owner);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(SegmentTextMatchTest, SealedJieBa) {
    auto schema = GenTestSchema({
        {"enable_match", "true"},
        {"enable_analyzer", "true"},
        {"analyzer_params", R"({"tokenizer": "jieba"})"},
    });
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}

TEST(SegmentTextMatchTest, SealedJieBaNullable) {
    auto schema = GenTestSchema(
        {
            {"enable_match", "true"},
            {"enable_tokenizer", "true"},
            {"analyzer_params", R"({"tokenizer": "jieba"})"},
        },
        true);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid = raw_data.raw_->mutable_fields_data()
                             ->at(1)
                             .mutable_scalars()
                             ->mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

// Test that growing segment loading flushed binlogs will build text match index.
TEST(SegmentTextMatchTest, GrowingLoadData) {
    milvus::exec::expression::FunctionFactory& factory =
        milvus::exec::expression::FunctionFactory::Instance();
    factory.Initialize();
    int64_t N = 7;
    auto schema = GenTestSchema({}, true);
    schema->AddField(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football",
                                        "golf",
                                        "",
                                        "baseball",
                                        "kungfu, football",
                                        ""};
    auto raw_data = DataGen(schema, N);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid = raw_data.raw_->mutable_fields_data()
                             ->at(1)
                             .mutable_scalars()
                             ->mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = true;
    }
    // so we cannot match the second row
    str_col_valid->at(1) = false;

    auto storage_config = get_default_local_storage_config();
    auto cm = storage::CreateChunkManager(storage_config);
    auto load_info = PrepareInsertBinlog(1, 2, 3, raw_data, cm);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto status = LoadFieldData(segment.get(), &load_info);
    ASSERT_EQ(status.error_code, Success);
    ASSERT_EQ(segment->get_real_count(), N);
    ASSERT_NE(segment->get_field_avg_size(FieldId(101)), 0);

    // Check whether the text index has been built.
    auto expr = GetMatchExpr(schema, "football", OpType::TextMatch);
    BitsetType final;
    final = ExecuteQueryExpr(expr, segment.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    ASSERT_TRUE(final[0]);
    ASSERT_FALSE(final[1]);
    ASSERT_FALSE(final[2]);
    ASSERT_FALSE(final[3]);
    ASSERT_FALSE(final[4]);
    ASSERT_TRUE(final[5]);
    ASSERT_FALSE(final[6]);
}

TEST(SegmentTextMatchTest, ConcurrentReadWriteWithNull) {
    auto schema = GenTestSchema({}, true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int64_t N = 1000;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col_valid = raw_data.raw_->mutable_fields_data()
                             ->at(1)
                             .mutable_scalars()
                             ->mutable_valid_data();
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N - 1; i++) {
        str_col->at(i) = "";
    }
    str_col->at(N - 1) = "football";
    for (int64_t i = 0; i < N - 1; i++) {
        str_col_valid->at(i) = false;
    }
    str_col_valid->at(N - 1) = true;

    std::thread writer([&seg, &raw_data, N]() {
        seg->PreInsert(N);
        seg->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.owner);
    });

    std::thread reader([&seg, &schema, N]() {
        auto start = std::chrono::high_resolution_clock::now();
        const std::chrono::seconds timeout_duration{2};
        while (true) {
            if (std::chrono::high_resolution_clock::now() - start >
                timeout_duration) {
                ASSERT_TRUE(false)
                    << "Failed to get valid results within timeout";
                break;
            }
            // Mirror production queries: reserved rows are not queryable
            // until the growing segment's visibility ACK advances.
            const auto active_count = seg->get_active_count(MAX_TIMESTAMP);
            if (active_count == 0) {
                std::this_thread::yield();
                continue;
            }
            auto expr = GetMatchExpr(schema, "football", OpType::TextMatch);
            auto final =
                ExecuteQueryExpr(expr, seg.get(), active_count, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), active_count);
            if (final.size() != N || !final[N - 1]) {
                continue;
            }
            for (int64_t i = 0; i < N - 1; i++) {
                ASSERT_FALSE(final[i]);
            }
            ASSERT_TRUE(final[N - 1]);
            break;
        }
    });

    writer.join();
    reader.join();
}

TEST(SegmentTextMatchTest, UpdatedRowsUseNativeAnalyzerAndSnapshot) {
    auto directory =
        storage::LocalDirectory::CreateOwned(TestRemotePath,
                                             "text-update-dictionary-XXXXXX",
                                             "text update dictionary");
    auto dictionary = std::filesystem::path(directory->Path()) / "stop.txt";
    {
        std::ofstream output(dictionary);
        output << "discarded\n";
        ASSERT_TRUE(output.good());
    }
    Config analyzer = {
        {"tokenizer", "jieba"},
        {"filter",
         {{{"type", "stop"},
           {"stop_words_file",
            {{"type", "local"}, {"path", dictionary.string()}}}}}}};
    auto schema = GenTestSchema({{"enable_match", "true"},
                                 {"enable_analyzer", "true"},
                                 {"analyzer_params", analyzer.dump()}},
                                true);
    auto make = [&](const std::vector<std::optional<std::string>>& rows) {
        auto data = DataGen(schema, rows.size());
        auto* field = data.raw_->mutable_fields_data()->Mutable(1);
        for (size_t i = 0; i < rows.size(); ++i) {
            field->mutable_scalars()->mutable_string_data()->set_data(
                i, rows[i].value_or(""));
            field->mutable_scalars()->set_valid_data(i, rows[i].has_value());
        }
        auto seg = CreateSealedWithFieldDataLoaded(schema, data);
        seg->CreateTextIndex(FieldId(101));
        return seg;
    };
    auto changed = make({"青铜时代", "swimming football", std::nullopt});
    auto initial = make({"青铜时代", "swimming football", std::nullopt});
    auto mirror =
        make({"swimming fast football discarded", std::nullopt, "青铜时代"});
    ASSERT_TRUE(std::filesystem::remove(dictionary));
    for (int i = 0; i < 3; ++i) {
        IdArray ids;
        ids.mutable_int_id()->add_data(i);
        UpdateAssignment a{FieldId(101), {}};
        if (i == 1)
            a.literal.set_null_value(google::protobuf::NULL_VALUE);
        else
            a.literal.mutable_scalar_value()->set_string_data(
                i == 0 ? "swimming fast football discarded" : "青铜时代");
        changed->ApplyUpdates(ids, 50, i, {a});
    }
    for (auto ts : {50, 49, 60}) {
        auto* expected = ts < 50 ? initial.get() : mirror.get();
        for (const auto& [op, query, option] :
             std::vector<std::tuple<OpType, std::string, int>>{
                 {OpType::TextMatch, "discarded", 1},
                 {OpType::TextMatch, "青铜", 1},
                 {OpType::TextMatch, "swimming football", 2},
                 {OpType::PhraseMatch, "swimming football", 0},
                 {OpType::PhraseMatch, "swimming football", 1},
                 {OpType::TextMatchFuzzy, "footbal", 1}}) {
            for (bool negate : {false, true}) {
                auto expr = negate ? GetNotMatchExpr(schema, query, op, option)
                                   : GetMatchExpr(schema, query, op, option);
                auto actual = ExecuteQueryExpr(expr, changed.get(), 3, ts);
                auto gold = ExecuteQueryExpr(expr, expected, 3, ts);
                for (size_t row = 0; row < 3; ++row)
                    EXPECT_EQ(actual[row], gold[row])
                        << ts << "/" << op << "/" << negate << "/" << row;
            }
        }
    }
}
