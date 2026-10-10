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

}  // namespace
}  // namespace milvus::index::test
