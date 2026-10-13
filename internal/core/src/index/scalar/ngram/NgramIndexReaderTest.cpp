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

#include "index/contracts/query/INgramReader.h"
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
