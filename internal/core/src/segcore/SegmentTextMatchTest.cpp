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
#include <thread>
#include "query/ExecPlanNodeVisitor.h"
#include "exec/expression/function/FunctionFactory.h"
#include "segcore/segment_c.h"
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
