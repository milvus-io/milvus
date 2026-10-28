// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "tantivy-wrapper.h"
#include "index/Utils.h"
#include "storage/artifact/LocalDirectory.h"
#include "test_utils/Constants.h"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/RegexQuery.h"
#include "index/contracts/query/INgramReader.h"
#include "index/scalar/ngram/NgramRegex.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

constexpr auto kBackend = "NgramVarcharMin2Max4Heap";

struct ConcreteCandidateCase {
    std::string name;
    std::string dataset;
    PatternOp op;
    std::string literal;
    std::optional<std::vector<size_t>> initial_offsets;
    std::vector<size_t> expected_offsets;
};

const IndexTestCases&
ConcreteNgramCases() {
    static const auto cases = [] {
        IndexTestCases cases;
        const std::vector<ConcreteCandidateCase> table = {
            {.name = "PostfixPositionFalsePositives",
             .dataset = "NgramWiki",
             .op = PatternOp::PostfixMatch,
             .literal = "ool",
             .expected_offsets = {0, 1, 2, 3, 4}},
            {.name = "PrefixPositionFalsePositives",
             .dataset = "NgramCoreNullable",
             .op = PatternOp::PrefixMatch,
             .literal = "abc",
             .expected_offsets = {23, 24, 25, 26}},
            {.name = "AndMergesSparseInitialMask",
             .dataset = "NgramCoreNullable",
             .op = PatternOp::InnerMatch,
             .literal = "hello",
             .initial_offsets = {{0, 2, 4, 6, 22, 30}},
             .expected_offsets = {0, 2, 4}},
            // Regex alternation: AND within a branch, union across branches.
            {.name = "RegexAlternationUnionsBranches",
             .dataset = "NgramCoreNullable",
             .op = PatternOp::RegexMatch,
             .literal = "tested|banana",
             .expected_offsets = {7, 13}},
            // `\|` is a literal byte: "banana|split" occurs in no row, so only
            // the second branch contributes candidates.
            {.name = "RegexEscapedPipeIsLiteral",
             .dataset = "NgramCoreNullable",
             .op = PatternOp::RegexMatch,
             .literal = "banana\\|split|tested",
             .expected_offsets = {7}},
            {.name = "RegexNestedAlternationIntersectsBranches",
             .dataset = "NgramCoreNullable",
             .op = PatternOp::RegexMatch,
             .literal = "(?:tes|ban)(?:ted|ana)",
             .expected_offsets = {7, 13}},
            {.name = "RegexAlternationAndMergesInitialMask",
             .dataset = "NgramCoreNullable",
             .op = PatternOp::RegexMatch,
             .literal = "tested|banana",
             .initial_offsets = {{0, 7, 22}},
             .expected_offsets = {7}},
        };
        for (const auto& test_case : table) {
            cases.Add(IndexTestCase<std::string_view>{
                .name = test_case.name,
                .dataset = test_case.dataset,
                .input_shape = BackendInputShape::Scalar,
                .domain = Domain::Row,
                .input_lifetime = InputLifetime::ReleaseBeforeBody,
                .backends = {kBackend},
                .body =
                    Observe<std::string_view>{
                        .capability = &ReaderCaps::ngram_candidates,
                        .run =
                            [test_case](
                                const ReaderBackend&,
                                const ScalarTestData<std::string_view>& data,
                                IIndexReaderBasePtr& reader) {
                                const auto* ngram =
                                    dynamic_cast<const INgramReader*>(
                                        reader.get());
                                ASSERT_NE(ngram, nullptr);
                                ASSERT_TRUE(ngram->CanHandle(test_case.literal,
                                                             test_case.op));
                                auto candidates =
                                    test_case.initial_offsets.has_value()
                                        ? Hits(data.values.size(),
                                               *test_case.initial_offsets)
                                        : TargetBitmap(data.values.size(),
                                                       true);
                                ngram->Candidates(test_case.literal,
                                                  test_case.op,
                                                  candidates);
                                const auto expected =
                                    Hits(data.values.size(),
                                         test_case.expected_offsets);
                                ExpectBitmap(candidates, expected);
                            },
                    },
            });
        }
        return cases;
    }();
    return cases;
}

class NgramIndexReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(NgramIndexReaderTest, ReturnsCurrentPhaseOneCandidates) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(NgramHeap,
                         NgramIndexReaderTest,
                         ::testing::ValuesIn(ConcreteNgramCases().All()),
                         FilterParamName);

using Condition = RegexLiteralCondition;

TEST(NgramRegexPlanTest, TrimToUtf8KeepsWholeCharacters) {
    const std::string zhong = "\xe4\xb8\xad";  // 中
    const std::string wen = "\xe6\x96\x87";    // 文
    EXPECT_EQ(TrimToUtf8("abc"), "abc");
    EXPECT_EQ(TrimToUtf8(zhong + wen), zhong + wen);
    // RE2 may cut a bounded atom inside a character at either end.
    EXPECT_EQ(TrimToUtf8(zhong.substr(1) + wen), wen);
    EXPECT_EQ(TrimToUtf8(wen + zhong.substr(0, 2)), wen);
    EXPECT_EQ(TrimToUtf8(zhong.substr(2)), "");
    // Invalid bytes end the usable prefix; surrogates are not valid UTF-8.
    EXPECT_EQ(TrimToUtf8("ab\xff" + wen), "ab");
    EXPECT_EQ(TrimToUtf8("\xed\xa0\x80" + wen), "");
    EXPECT_EQ(TrimToUtf8(std::string("a\0b", 3)), std::string("a\0b", 3));
}

TEST(NgramRegexPlanTest, ProjectsConditionOntoMinGram) {
    const auto literal = [](const std::string& text) {
        return Condition{Condition::Op::Literal, text, {}};
    };
    const auto node = [](Condition::Op op, std::vector<Condition> children) {
        return Condition{op, {}, std::move(children)};
    };
    using Op = Condition::Op;
    // A short literal inside an AND is dropped; the AND keeps the rest.
    EXPECT_EQ(PlanNgramRegexCondition(
                  node(Op::And, {literal("ab"), literal("hello")}), 3),
              literal("hello"));
    // A short OR branch can match any row, so the whole OR is TRUE.
    EXPECT_TRUE(PlanNgramRegexCondition(
                    node(Op::Or, {literal("ab"), literal("hello")}), 3)
                    .IsTrue());
    EXPECT_TRUE(PlanNgramRegexCondition(
                    node(Op::Or, {Condition{}, literal("hello")}), 2)
                    .IsTrue());
    EXPECT_EQ(PlanNgramRegexCondition(
                  node(Op::Or, {literal("ab"), literal("hello")}), 2),
              node(Op::Or, {literal("ab"), literal("hello")}));
    // ... but an AND above it survives through its other children.
    EXPECT_EQ(PlanNgramRegexCondition(
                  node(Op::And,
                       {node(Op::Or, {literal("ab"), literal("hello")}),
                        literal("world")}),
                  3),
              literal("world"));
    // Length is counted in UTF-8 characters, after trimming partial ones.
    EXPECT_EQ(
        PlanNgramRegexCondition(literal("\xb8\xad\xe6\x96\x87\xe5\xad\x97"), 2),
        literal("\xe6\x96\x87\xe5\xad\x97"));
    EXPECT_TRUE(PlanNgramRegexCondition(literal("\xe6\x96\x87"), 2).IsTrue());
    // A tree over the shared node budget is rejected, never truncated.
    std::vector<Condition> many;
    for (int i = 0; i < 200; ++i) {
        many.push_back(literal("w" + std::to_string(1000 + i)));
    }
    EXPECT_TRUE(
        PlanNgramRegexCondition(node(Op::Or, std::move(many)), 2).IsTrue());
}

TEST(NgramRegexPlanTest, PlannedAtomsAreValidUtf8) {
    std::string long_han;
    for (int i = 0; i < 30; ++i) {
        long_han += "\xe4\xb8\xad";  // 90 bytes, cut at 64 by RE2 atoms
    }
    const auto plan = PlanNgramRegex(long_han + "|tested", 2);
    ASSERT_FALSE(plan.IsTrue());
    std::vector<const Condition*> pending{&plan};
    size_t literals = 0;
    while (!pending.empty()) {
        const auto* current = pending.back();
        pending.pop_back();
        if (current->op == Condition::Op::Literal) {
            ++literals;
            EXPECT_EQ(TrimToUtf8(current->literal), current->literal);
            EXPECT_GE(
                Utf8CharCount(current->literal.data(), current->literal.size()),
                2);
        }
        for (const auto& child : current->children) {
            pending.push_back(&child);
        }
    }
    EXPECT_GE(literals, 2);
}

TEST(NgramRegexPlanTest, AppliesBranchesAsUnionOfIntersections) {
    const std::vector<std::string> rows = {
        "foo bar", "foo", "bar", "baz qux", "qux", "foo qux"};
    const auto apply = [&rows](const std::vector<std::string>& literals,
                               TargetBitmap& bitmap) {
        for (const auto& literal : literals) {
            for (size_t row = 0; row < rows.size(); ++row) {
                if (rows[row].find(literal) == std::string::npos) {
                    bitmap.reset(row);
                }
            }
        }
    };
    const auto run = [&](const std::string& pattern,
                         std::optional<std::vector<size_t>> initial) {
        auto candidates = initial.has_value() ? Hits(rows.size(), *initial)
                                              : TargetBitmap(rows.size(), true);
        ApplyNgramRegexPlan(PlanNgramRegex(pattern, 3), candidates, apply);
        return candidates;
    };
    ExpectBitmap(run("foo.*bar|baz.*qux", std::nullopt),
                 Hits(rows.size(), {0, 3}));
    ExpectBitmap(run("(?:foo|baz).*qux", std::nullopt),
                 Hits(rows.size(), {3, 5}));
    ExpectBitmap(run("foo|qux", std::vector<size_t>{0, 2, 4}),
                 Hits(rows.size(), {0, 4}));
    ExpectBitmap(run("foo|qux", std::vector<size_t>{}),
                 TargetBitmap(rows.size()));
}

TEST(NgramFfiBoundaryTest, EmbeddedNulCrossesFfiIntact) {
    auto directory = storage::LocalDirectory::CreateOwned(
        TestLocalPath, "ngram-nul-ffi-XXXXXX", "ngram test");
    const auto& path = directory->Path();

    milvus::tantivy::TantivyIndexWrapper wrapper(
        "ngram", path.c_str(), uintptr_t{2}, uintptr_t{4});
    std::vector<std::string> data = {std::string("xab\0cy", 6), "xabzz"};
    wrapper.add_data<std::string>(data.data(), data.size(), 0);
    wrapper.finish();
    wrapper.create_reader(milvus::index::SetBitsetSealed);

    const std::string nul_literal("ab\0c", 4);
    {
        TargetBitmap bitset(data.size());
        wrapper.ngram_match_query(nul_literal, 2, 4, &bitset);
        EXPECT_TRUE(bitset[0]);
        EXPECT_FALSE(bitset[1]) << "literal truncated at NUL to \"ab\"";
    }
    {
        auto terms = wrapper.ngram_tokenize({nul_literal}, 2, 4);
        ASSERT_EQ(terms.size(), 1);
        EXPECT_EQ(terms[0], nul_literal);
        TargetBitmap bitset(data.size());
        wrapper.ngram_term_posting_list(terms[0], &bitset);
        EXPECT_TRUE(bitset[0]);
        EXPECT_FALSE(bitset[1]);
    }
    {
        // longer than max_gram: split into 4-char grams, each carrying the NUL
        auto terms = wrapper.ngram_tokenize({data[0]}, 2, 4);
        ASSERT_EQ(terms.size(), 3);
        for (const auto& term : terms) {
            EXPECT_EQ(term.size(), 4) << "term lost bytes at NUL";
            EXPECT_NE(term.find('\0'), std::string::npos);
        }
    }
}

// The C++ gate above is the primary defense; this pins the second one: a
// literal that reaches the rust binding below min_gram must come back as a
// SegcoreError, not a panic. The binding is entered through `extern "C"`
// frames, where a panic cannot unwind and aborts the QueryNode.
TEST(NgramFfiBoundaryTest, ShortLiteralAtRustBoundaryThrows) {
    auto directory = storage::LocalDirectory::CreateOwned(
        TestLocalPath, "ngram-short-ffi-XXXXXX", "ngram test");
    const auto& path = directory->Path();

    // explicit uintptr_t: the (field, path, version, in_ram) json-stats
    // constructor would otherwise make the int literals ambiguous
    milvus::tantivy::TantivyIndexWrapper wrapper(
        "ngram", path.c_str(), uintptr_t{2}, uintptr_t{3});
    std::vector<std::string> data = {"订单（已取消）", "hello world"};
    wrapper.add_data<std::string>(data.data(), data.size(), 0);
    wrapper.finish();
    wrapper.create_reader(milvus::index::SetBitsetSealed);

    const std::string one_char_three_bytes = "订";
    ASSERT_EQ(one_char_three_bytes.size(), 3);

    {
        TargetBitmap bitset(data.size());
        EXPECT_THROW(
            wrapper.ngram_match_query(one_char_three_bytes, 2, 3, &bitset),
            milvus::SegcoreError);
        EXPECT_THROW(wrapper.ngram_match_query("a", 2, 3, &bitset),
                     milvus::SegcoreError);
    }
    EXPECT_THROW(wrapper.ngram_tokenize({one_char_three_bytes}, 2, 3),
                 milvus::SegcoreError);
    EXPECT_THROW(wrapper.ngram_tokenize({"订单", "a"}, 2, 3),
                 milvus::SegcoreError);
    EXPECT_THROW(wrapper.ngram_tokenize({}, 2, 3), milvus::SegcoreError);

    // the reader is still usable afterwards
    TargetBitmap bitset(data.size());
    wrapper.ngram_match_query("订单", 2, 3, &bitset);
    EXPECT_TRUE(bitset[0]);
    EXPECT_FALSE(bitset[1]);
    EXPECT_FALSE(wrapper.ngram_tokenize({"订单"}, 2, 3).empty());
}

}  // namespace
}  // namespace milvus::index::test
