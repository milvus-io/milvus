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
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "index/Meta.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

enum class TextQueryKind {
    Match,
    Phrase,
    Fuzzy,
};

struct TextQueryArgs {
    TextQueryKind kind;
    std::string query;
    uint32_t parameter;
};

using BackendSelector = std::function<bool(const ReaderBackend&)>;

TargetBitmap
RunTextQuery(const ITextMatchReader& reader, const TextQueryArgs& args) {
    switch (args.kind) {
        case TextQueryKind::Match:
            return reader.MatchQuery(args.query, args.parameter);
        case TextQueryKind::Phrase:
            return reader.PhraseMatchQuery(args.query, args.parameter);
        case TextQueryKind::Fuzzy:
            return reader.FuzzyMatchQuery(args.query, args.parameter);
    }
    throw std::logic_error("unknown text query kind");
}

bool
UsesJieba(const ReaderBackend& backend) {
    const auto& params = backend.BuildParams();
    if (!params.contains("analyzer_params") ||
        !params.at("analyzer_params").is_string()) {
        return false;
    }
    const auto analyzer =
        Config::parse(params.at("analyzer_params").get<std::string>());
    return analyzer.is_object() && analyzer.value("tokenizer", "") == "jieba";
}

const BackendSelector kStandardAnalyzer = [](const ReaderBackend& backend) {
    return !UsesJieba(backend);
};
const BackendSelector kJiebaAnalyzer = UsesJieba;

void
AddTextCase(IndexTestCases& cases,
            std::string name,
            std::string dataset,
            TextQueryArgs args,
            std::vector<size_t> expected_offsets,
            BackendSelector select_backend) {
    cases.Add(IndexTestCase<std::string_view>{
        .name = std::move(name),
        .dataset = std::move(dataset),
        .input_shape = BackendInputShape::Scalar,
        .domain = Domain::Row,
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .select_backend = std::move(select_backend),
        .body =
            Observe<std::string_view>{
                .capability = &ReaderCaps::text_match,
                .run =
                    [args = std::move(args),
                     expected_offsets = std::move(expected_offsets)](
                        const ReaderBackend&,
                        const ScalarTestData<std::string_view>& data,
                        IIndexReaderBasePtr& reader) {
                        ASSERT_TRUE(reader->Caps().text_match);
                        EXPECT_TRUE(reader->Caps().exact);
                        const auto* text_reader =
                            dynamic_cast<const ITextMatchReader*>(reader.get());
                        ASSERT_NE(text_reader, nullptr);

                        const auto expected =
                            Hits(data.values.size(), expected_offsets);
                        auto actual = RunTextQuery(*text_reader, args);
                        ExpectBitmap(actual, expected);
                        ExpectNullState(data, *reader);
                    },
            },
    });
}

void
AddStandardEnglishCases(IndexTestCases& cases) {
    constexpr auto dataset = "TextEnglishNullable";
    AddTextCase(cases,
                "MatchFootballMin1",
                dataset,
                {TextQueryKind::Match, "football", 1},
                {0, 2, 9, 11},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchNothingMin1",
                dataset,
                {TextQueryKind::Match, "nothing", 1},
                {},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchTwoOfThree",
                dataset,
                {TextQueryKind::Match, "football pingpang cricket", 2},
                {0, 9},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchThreeOfThree",
                dataset,
                {TextQueryKind::Match, "football basketball pingpang", 3},
                {0},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchTwoTermsMin1",
                dataset,
                {TextQueryKind::Match, "basketball swimming", 1},
                {0, 2, 8, 11},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchTwoTermsMin2",
                dataset,
                {TextQueryKind::Match, "basketball swimming", 2},
                {8},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchEmptyQuery",
                dataset,
                {TextQueryKind::Match, "", 1},
                {},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchSecondBatchFoo",
                dataset,
                {TextQueryKind::Match, "foo", 1},
                {4},
                kStandardAnalyzer);
    AddTextCase(cases,
                "MatchThirdBatchBar",
                dataset,
                {TextQueryKind::Match, "bar", 1},
                {6},
                kStandardAnalyzer);

    AddTextCase(cases,
                "PhraseFootballSlop0",
                dataset,
                {TextQueryKind::Phrase, "football", 0},
                {0, 2, 9, 11},
                kStandardAnalyzer);
    AddTextCase(cases,
                "PhraseSwimmingFootballSlop0",
                dataset,
                {TextQueryKind::Phrase, "swimming football", 0},
                {2},
                kStandardAnalyzer);
    AddTextCase(cases,
                "PhraseFootballSwimmingSlop0",
                dataset,
                {TextQueryKind::Phrase, "football swimming", 0},
                {11},
                kStandardAnalyzer);
    AddTextCase(cases,
                "PhraseFootballSwimmingSlop1",
                dataset,
                {TextQueryKind::Phrase, "football swimming", 1},
                {11},
                kStandardAnalyzer);
    AddTextCase(cases,
                "PhraseFootballSwimmingSlop2",
                dataset,
                {TextQueryKind::Phrase, "football swimming", 2},
                {2, 11},
                kStandardAnalyzer);
    AddTextCase(cases,
                "PhraseFootballPingpangSlop0",
                dataset,
                {TextQueryKind::Phrase, "football pingpang", 0},
                {9},
                kStandardAnalyzer);
    AddTextCase(cases,
                "PhraseFootballPingpangSlop1",
                dataset,
                {TextQueryKind::Phrase, "football pingpang", 1},
                {0, 9},
                kStandardAnalyzer);

    AddTextCase(cases,
                "FuzzyFootbalDistance0",
                dataset,
                {TextQueryKind::Fuzzy, "footbal", 0},
                {},
                kStandardAnalyzer);
    AddTextCase(cases,
                "FuzzyFootbalDistance1",
                dataset,
                {TextQueryKind::Fuzzy, "footbal", 1},
                {0, 2, 9, 11},
                kStandardAnalyzer);
    AddTextCase(cases,
                "FuzzyFotbalDistance1",
                dataset,
                {TextQueryKind::Fuzzy, "fotbal", 1},
                {},
                kStandardAnalyzer);
    AddTextCase(cases,
                "FuzzyFotbalDistance2",
                dataset,
                {TextQueryKind::Fuzzy, "fotbal", 2},
                {0, 2, 9, 11},
                kStandardAnalyzer);
}

void
AddAllValidCases(IndexTestCases& cases) {
    constexpr auto dataset = "TextEnglishAllValid";
    AddTextCase(cases,
                "AllValidMatchFootball",
                dataset,
                {TextQueryKind::Match, "football", 1},
                {0, 1, 3, 5},
                kStandardAnalyzer);
    AddTextCase(cases,
                "AllValidPhraseSwimmingFootball",
                dataset,
                {TextQueryKind::Phrase, "swimming football", 0},
                {1},
                kStandardAnalyzer);
    AddTextCase(cases,
                "AllValidFuzzyBasketbal",
                dataset,
                {TextQueryKind::Fuzzy, "basketbal", 1},
                {0, 2},
                kStandardAnalyzer);
}

void
AddNullEmptyAndUnicodeCases(IndexTestCases& cases) {
    AddTextCase(cases,
                "SingleBatchNullableAlpha",
                "TextSingleBatchNullable",
                {TextQueryKind::Match, "alpha", 1},
                {0},
                kStandardAnalyzer);
    AddTextCase(cases,
                "SingleBatchNullableBeta",
                "TextSingleBatchNullable",
                {TextQueryKind::Match, "beta", 1},
                {2},
                kStandardAnalyzer);
    AddTextCase(cases,
                "AllNullMatch",
                "TextAllNull",
                {TextQueryKind::Match, "football", 1},
                {},
                kStandardAnalyzer);
    AddTextCase(cases,
                "AllNullPhrase",
                "TextAllNull",
                {TextQueryKind::Phrase, "青铜时代", 0},
                {},
                kStandardAnalyzer);
    AddTextCase(cases,
                "EmptyReaderMatch",
                "TextEmpty",
                {TextQueryKind::Match, "football", 1},
                {},
                kStandardAnalyzer);
    AddTextCase(cases,
                "UnicodeCafe",
                "TextUnicodeNullable",
                {TextQueryKind::Match, "café", 1},
                {0, 1},
                kStandardAnalyzer);
    AddTextCase(cases,
                "UnicodeEmoji",
                "TextUnicodeNullable",
                {TextQueryKind::Match, "emoji", 1},
                {4, 5},
                kStandardAnalyzer);
    AddTextCase(cases,
                "PunctuationToken",
                "TextUnicodeNullable",
                {TextQueryKind::Match, "football", 1},
                {6},
                kStandardAnalyzer);
    AddTextCase(cases,
                "LongValueNoFalseMatch",
                "TextUnicodeNullable",
                {TextQueryKind::Match, "nothing", 1},
                {},
                kStandardAnalyzer);
}

void
AddJiebaCases(IndexTestCases& cases) {
    constexpr auto dataset = "TextJiebaNullable";
    AddTextCase(cases,
                "JiebaMatchBronze",
                dataset,
                {TextQueryKind::Match, "青铜", 1},
                {0},
                kJiebaAnalyzer);
    AddTextCase(cases,
                "JiebaMatchGold",
                dataset,
                {TextQueryKind::Match, "黄金", 1},
                {1, 3},
                kJiebaAnalyzer);
    AddTextCase(cases,
                "JiebaMatchEra",
                dataset,
                {TextQueryKind::Match, "时代", 1},
                {0, 1, 2},
                kJiebaAnalyzer);
    AddTextCase(cases,
                "JiebaPhraseBronze",
                dataset,
                {TextQueryKind::Phrase, "青铜", 0},
                {0},
                kJiebaAnalyzer);
    AddTextCase(cases,
                "JiebaPhraseGold",
                dataset,
                {TextQueryKind::Phrase, "黄金", 0},
                {1, 3},
                kJiebaAnalyzer);
    AddTextCase(cases,
                "JiebaPhraseGoldEra",
                dataset,
                {TextQueryKind::Phrase, "黄金时代", 0},
                {1},
                kJiebaAnalyzer);
    AddTextCase(cases,
                "JiebaPhraseCommonEra",
                dataset,
                {TextQueryKind::Phrase, "时代", 0},
                {0, 1, 2},
                kJiebaAnalyzer);
}

const IndexTestCases&
TextCases() {
    static const auto cases = [] {
        IndexTestCases cases;
        AddStandardEnglishCases(cases);
        AddAllValidCases(cases);
        AddNullEmptyAndUnicodeCases(cases);
        AddJiebaCases(cases);
        return cases;
    }();
    return cases;
}

class TextMatchReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(TextMatchReaderTest, MatchesManualTokenOffsets) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(TextBackends,
                         TextMatchReaderTest,
                         ::testing::ValuesIn(TextCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
