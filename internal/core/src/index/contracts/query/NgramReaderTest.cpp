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

#include <algorithm>
#include <cstddef>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/JsonCastType.h"
#include "index/Meta.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INgramReader.h"
#include "index/scalar/ngram/JsonProjectedString.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

using BackendSelector = std::function<bool(const ReaderBackend&)>;

struct NgramCase {
    PatternOp op;
    std::string literal;
    bool can_handle{true};
    // nullopt starts with every row; an empty vector starts with no candidates.
    std::optional<std::vector<size_t>> initial_offsets;
    // Exact predicate hits that Phase 1 must retain when present initially.
    std::vector<size_t> exact_offsets;
};

size_t
ConfiguredMinGram(const ReaderBackend& backend) {
    const auto& value = backend.BuildParams().at(MIN_GRAM);
    if (value.is_number_unsigned()) {
        return value.get<size_t>();
    }
    if (value.is_number_integer()) {
        return static_cast<size_t>(value.get<int64_t>());
    }
    if (value.is_string()) {
        return std::stoull(value.get<std::string>());
    }
    throw std::logic_error("NGRAM min_gram test parameter is not integral");
}

const BackendSelector kMinGram2 = [](const ReaderBackend& backend) {
    return ConfiguredMinGram(backend) == 2;
};
const BackendSelector kMinGram3 = [](const ReaderBackend& backend) {
    return ConfiguredMinGram(backend) == 3;
};

template <typename T>
void
ObserveNgram(const NgramCase& test_case,
             const ScalarTestData<T>& data,
             IIndexReaderBasePtr& reader) {
    ASSERT_TRUE(reader->Caps().ngram_candidates);
    EXPECT_FALSE(reader->Caps().exact);

    JsonResolvedReader resolved;
    const IIndexReaderBase* query_reader = reader.get();
    if constexpr (std::is_same_v<T, JsonProjectedString>) {
        ASSERT_TRUE(reader->Caps().json_paths);
        const auto* json_reader =
            dynamic_cast<const IJsonIndexReader*>(reader.get());
        ASSERT_NE(json_reader, nullptr);
        const auto cast = JsonCastType::FromString("VARCHAR");
        const auto casts = json_reader->CastTypesOf("/a");
        EXPECT_TRUE(std::any_of(casts.begin(), casts.end(), [&](const auto& c) {
            return c.data_type() == cast.data_type() &&
                   c.element_type() == cast.element_type();
        }));
        resolved = json_reader->Resolve("/a", cast);
        ASSERT_TRUE(resolved);
        query_reader = resolved.get();

        const auto expected_exists =
            Hits(data.values.size(), {2, 3, 4, 5, 6, 7, 9});
        auto actual_exists = json_reader->Exists("/a");
        ExpectBitmap(actual_exists, expected_exists);
    }

    const auto* ngram = dynamic_cast<const INgramReader*>(query_reader);
    ASSERT_NE(ngram, nullptr);
    ExpectNullState(data, *query_reader);

    EXPECT_EQ(ngram->CanHandle(test_case.literal, test_case.op),
              test_case.can_handle);
    if (!test_case.can_handle) {
        return;
    }

    auto candidates = test_case.initial_offsets.has_value()
                          ? Hits(data.values.size(), *test_case.initial_offsets)
                          : TargetBitmap(data.values.size(), true);
    const auto initial =
        test_case.initial_offsets.has_value()
            ? Hits(data.values.size(), *test_case.initial_offsets)
            : TargetBitmap(data.values.size(), true);
    ngram->Candidates(test_case.literal, test_case.op, candidates);
    ASSERT_EQ(candidates.size(), data.values.size());

    for (size_t i = 0; i < candidates.size(); ++i) {
        EXPECT_FALSE(candidates[i] && !initial[i])
            << "NGRAM introduced candidate offset " << i;
    }
    for (const auto offset : test_case.exact_offsets) {
        ASSERT_LT(offset, candidates.size());
        if (initial[offset]) {
            EXPECT_TRUE(candidates[offset])
                << "NGRAM dropped exact hit offset " << offset;
        }
    }
}

template <typename T>
void
AddNgramCase(IndexTestCases& cases,
             std::string name,
             std::string dataset,
             NgramCase test_case,
             BackendSelector select_backend = {}) {
    constexpr auto shape = std::is_same_v<T, JsonProjectedString>
                               ? BackendInputShape::JsonProjected
                               : BackendInputShape::Scalar;
    cases.Add(IndexTestCase<T>{
        .name = std::move(name),
        .dataset = std::move(dataset),
        .input_shape = shape,
        .domain = Domain::Row,
        .logical_value_type = std::is_same_v<T, JsonProjectedString>
                                  ? std::optional<DataType>(DataType::VARCHAR)
                                  : std::nullopt,
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .select_backend = std::move(select_backend),
        .body =
            Observe<T>{
                .capability = &ReaderCaps::ngram_candidates,
                .run =
                    [test_case = std::move(test_case)](
                        const ReaderBackend&,
                        const ScalarTestData<T>& data,
                        IIndexReaderBasePtr& reader) {
                        ObserveNgram(test_case, data, reader);
                    },
            },
    });
}

void
AddCanHandleCases(IndexTestCases& cases) {
    constexpr auto dataset = "NgramCoreNullable";
    AddNgramCase<std::string_view>(
        cases,
        "CanHandleMatchAllLongParts",
        dataset,
        {.op = PatternOp::Match, .literal = "%ary%sec%"});
    AddNgramCase<std::string_view>(
        cases,
        "DeclineMatchShortPart",
        dataset,
        {.op = PatternOp::Match, .literal = "%ary%s%", .can_handle = false});
    AddNgramCase<std::string_view>(
        cases,
        "DeclineWildcardOnly",
        dataset,
        {.op = PatternOp::Match, .literal = "%_%", .can_handle = false});
    AddNgramCase<std::string_view>(
        cases,
        "DeclineEmptyMatch",
        dataset,
        {.op = PatternOp::Match, .literal = "", .can_handle = false});

    for (const auto op : {PatternOp::PrefixMatch,
                          PatternOp::PostfixMatch,
                          PatternOp::InnerMatch}) {
        const auto op_name = op == PatternOp::PrefixMatch    ? "Prefix"
                             : op == PatternOp::PostfixMatch ? "Postfix"
                                                             : "Inner";
        AddNgramCase<std::string_view>(
            cases,
            std::string("DeclineOneCharacter") + op_name,
            dataset,
            {.op = op, .literal = "a", .can_handle = false});
        AddNgramCase<std::string_view>(
            cases,
            std::string("Min2AcceptsTwoCharacter") + op_name,
            dataset,
            {.op = op, .literal = "ab"},
            kMinGram2);
        AddNgramCase<std::string_view>(
            cases,
            std::string("Min3DeclinesTwoCharacter") + op_name,
            dataset,
            {.op = op, .literal = "ab", .can_handle = false},
            kMinGram3);
        AddNgramCase<std::string_view>(
            cases,
            std::string("AcceptsThreeCharacter") + op_name,
            dataset,
            {.op = op, .literal = "abc"});
    }

    AddNgramCase<std::string_view>(
        cases,
        "RegexUsableLiteral",
        dataset,
        {.op = PatternOp::RegexMatch, .literal = "^hello.*world$"});
    AddNgramCase<std::string_view>(
        cases,
        "RegexAnyUsableLiteral",
        dataset,
        {.op = PatternOp::RegexMatch, .literal = "a.*world"});
    AddNgramCase<std::string_view>(
        cases,
        "DeclineRegexShortLiterals",
        dataset,
        {.op = PatternOp::RegexMatch, .literal = "a.*b", .can_handle = false});
    AddNgramCase<std::string_view>(cases,
                                   "DeclineRegexAlternation",
                                   dataset,
                                   {.op = PatternOp::RegexMatch,
                                    .literal = "foo|hello",
                                    .can_handle = false});

    AddNgramCase<std::string_view>(
        cases,
        "Min2AcceptsTwoChineseCharacters",
        "NgramUtf8",
        {.op = PatternOp::InnerMatch, .literal = "你好"},
        kMinGram2);
    AddNgramCase<std::string_view>(
        cases,
        "Min2AcceptsTwoChineseCharactersMatch",
        "NgramUtf8",
        {.op = PatternOp::Match, .literal = "%你好%"},
        kMinGram2);
    AddNgramCase<std::string_view>(
        cases,
        "Min2AcceptsTwoChineseCharactersRegex",
        "NgramUtf8",
        {.op = PatternOp::RegexMatch, .literal = "你好"},
        kMinGram2);
    AddNgramCase<std::string_view>(
        cases,
        "Min3DeclinesTwoChineseCharacters",
        "NgramUtf8",
        {.op = PatternOp::InnerMatch, .literal = "你好", .can_handle = false},
        kMinGram3);
    for (const auto op : {PatternOp::InnerMatch,
                          PatternOp::PrefixMatch,
                          PatternOp::PostfixMatch,
                          PatternOp::RegexMatch}) {
        const auto op_name = op == PatternOp::InnerMatch     ? "Inner"
                             : op == PatternOp::PrefixMatch  ? "Prefix"
                             : op == PatternOp::PostfixMatch ? "Postfix"
                                                             : "Regex";
        AddNgramCase<std::string_view>(
            cases,
            std::string("DeclineOneChineseCharacter") + op_name,
            "NgramUtf8",
            {.op = op, .literal = "你", .can_handle = false});
    }
    AddNgramCase<std::string_view>(
        cases,
        "DeclineOneChineseCharacterMatch",
        "NgramUtf8",
        {.op = PatternOp::Match, .literal = "%你%", .can_handle = false});
}

void
AddWikiCases(IndexTestCases& cases) {
    constexpr auto dataset = "NgramWiki";
    AddNgramCase<std::string_view>(cases,
                                   "WikiInnerAry",
                                   dataset,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "ary",
                                       .exact_offsets = {0, 1, 2, 3, 4},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiInnerUppercaseYSpaceS",
                                   dataset,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "y S",
                                       .exact_offsets = {1, 3, 4},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiInnerLowercaseYSpaceS",
                                   dataset,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "y s",
                                       .exact_offsets = {0, 1, 2, 3},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiPrefixSir",
                                   dataset,
                                   {
                                       .op = PatternOp::PrefixMatch,
                                       .literal = "Sir",
                                       .exact_offsets = {3, 4},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiPostfixOolRetainsExactHit",
                                   dataset,
                                   {
                                       .op = PatternOp::PostfixMatch,
                                       .literal = "ool",
                                       .exact_offsets = {4},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiLikeAlvaradoAndSecondary",
                                   dataset,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%Alv%y s%",
                                       .exact_offsets = {0},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiInnerSecondarySchoolRetainsExactHits",
                                   dataset,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "secondary school",
                                       .exact_offsets = {1, 2, 3},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiPrefixSirWinston",
                                   dataset,
                                   {
                                       .op = PatternOp::PrefixMatch,
                                       .literal = "Sir Winston",
                                       .exact_offsets = {3, 4},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiPostfixGermany",
                                   dataset,
                                   {
                                       .op = PatternOp::PostfixMatch,
                                       .literal = "Germany.",
                                       .exact_offsets = {2},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "WikiLikeSecondaryThenSchool",
                                   dataset,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%secondary%school%",
                                       .exact_offsets = {0, 1, 2, 3},
                                   });
}

void
AddCoreCandidateCases(IndexTestCases& cases) {
    constexpr auto dataset = "NgramCoreNullable";
    AddNgramCase<std::string_view>(cases,
                                   "InnerSecondarySchoolRetainsExactHit",
                                   dataset,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "secondary school",
                                       .exact_offsets = {30},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "SimplePrefixElementary",
                                   dataset,
                                   {
                                       .op = PatternOp::PrefixMatch,
                                       .literal = "ele",
                                       .exact_offsets = {22},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "SimpleLikeAryThenSec",
                                   dataset,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%ary%sec%",
                                       .exact_offsets = {22},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "SimplePostfixAry",
                                   dataset,
                                   {
                                       .op = PatternOp::PostfixMatch,
                                       .literal = "ary",
                                       .exact_offsets = {22},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "PrefixRetainsExactHits",
                                   dataset,
                                   {
                                       .op = PatternOp::PrefixMatch,
                                       .literal = "abc",
                                       .exact_offsets = {23, 25},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "PostfixRetainsExactHits",
                                   dataset,
                                   {
                                       .op = PatternOp::PostfixMatch,
                                       .literal = "abc",
                                       .exact_offsets = {23, 24},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "InnerAllPositions",
                                   dataset,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "abc",
                                       .exact_offsets = {23, 24, 25, 26},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "LikePrefixRetainsExactHits",
                                   dataset,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "hello%",
                                       .exact_offsets = {0, 1, 4},
                                   });
    AddNgramCase<std::string_view>(
        cases,
        "SparseInitialMask",
        dataset,
        {
            .op = PatternOp::InnerMatch,
            .literal = "hello",
            .initial_offsets = {{0, 2, 4, 6, 22, 30}},
            .exact_offsets = {0, 2, 4},
        });
    AddNgramCase<std::string_view>(cases,
                                   "EmptyInitialMask",
                                   dataset,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "hello",
                                       .initial_offsets = {{}},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "RegexRequiredLiterals",
                                   dataset,
                                   {
                                       .op = PatternOp::RegexMatch,
                                       .literal = "^hello.*world$",
                                       .exact_offsets = {1, 4},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "RegexCharacterClassQuery",
                                   dataset,
                                   {
                                       .op = PatternOp::RegexMatch,
                                       .literal = "^test[0-9]+ing$",
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "RegexEscapedClassUsesOtherLiteral",
                                   dataset,
                                   {
                                       .op = PatternOp::RegexMatch,
                                       .literal = "\\d+abc",
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "AllNullCandidates",
                                   "NgramAllNull",
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "hello",
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "EmptyReaderCandidates",
                                   "NgramEmpty",
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "hello",
                                   });
}

void
AddMatcherAgreementCases(IndexTestCases& cases) {
    constexpr auto dataset = "NgramCoreNullable";
    struct MatcherAgreementCase {
        const char* name;
        PatternOp op;
        const char* literal;
        std::vector<size_t> exact;
        bool min_gram_2_only{false};
    };
    const std::vector<MatcherAgreementCase> table = {
        {"PrefixHello", PatternOp::PrefixMatch, "hello", {0, 1, 4}},
        {"PrefixTest", PatternOp::PrefixMatch, "test", {6, 7, 8, 9}},
        {"PrefixApp", PatternOp::PrefixMatch, "app", {10, 11}},
        {"PrefixAb", PatternOp::PrefixMatch, "ab", {17, 18, 19, 23, 25}, true},
        {"PostfixWorld", PatternOp::PostfixMatch, "world", {1, 4}},
        {"PostfixIng", PatternOp::PostfixMatch, "ing", {6}},
        {"PostfixPle", PatternOp::PostfixMatch, "ple", {12}},
        {"PostfixAb", PatternOp::PostfixMatch, "ab", {16, 18, 19}, true},
        {"InnerEllo", PatternOp::InnerMatch, "ello", {0, 1, 2, 3, 4, 5}},
        {"InnerTest", PatternOp::InnerMatch, "test", {6, 7, 8, 9}},
        {"InnerApp", PatternOp::InnerMatch, "app", {10, 11, 12}},
        {"InnerAa",
         PatternOp::InnerMatch,
         "aa",
         {14, 15, 16, 17, 20, 21},
         true},
        {"InnerAb",
         PatternOp::InnerMatch,
         "ab",
         {16, 17, 18, 19, 23, 24, 25, 26},
         true},
        {"LikeHelloPrefix", PatternOp::Match, "hello%", {0, 1, 4}},
        {"LikeWorldPostfix", PatternOp::Match, "%world", {1, 4}},
        {"LikeElloInner", PatternOp::Match, "%ello%", {0, 1, 2, 3, 4, 5}},
        {"LikeTestIng", PatternOp::Match, "test%ing", {6}},
        {"LikeRepeatedAa", PatternOp::Match, "%aa%aa%", {14}, true},
        {"LikeAbPrefixPostfix", PatternOp::Match, "ab%ab", {18, 19}, true},
        {"LikeRepeatedAb", PatternOp::Match, "%ab%ab%", {18, 19}, true},
    };
    for (const auto& item : table) {
        BackendSelector selector;
        if (item.min_gram_2_only) {
            selector = kMinGram2;
        }
        AddNgramCase<std::string_view>(cases,
                                       item.name,
                                       dataset,
                                       {.op = item.op,
                                        .literal = item.literal,
                                        .exact_offsets = item.exact},
                                       std::move(selector));
        if (item.min_gram_2_only && item.op == PatternOp::Match) {
            AddNgramCase<std::string_view>(
                cases,
                std::string(item.name) + "DeclinesMin3",
                dataset,
                {.op = item.op, .literal = item.literal, .can_handle = false},
                kMinGram3);
        }
    }
}

void
AddOverlapCases(IndexTestCases& cases) {
    constexpr auto dataset = "NgramOverlap";
    AddNgramCase<std::string_view>(cases,
                                   "OverlapTwoAa",
                                   dataset,
                                   {.op = PatternOp::Match,
                                    .literal = "%aa%aa%",
                                    .exact_offsets = {2, 3, 4, 14}},
                                   kMinGram2);
    AddNgramCase<std::string_view>(
        cases,
        "OverlapThreeAa",
        dataset,
        {.op = PatternOp::Match, .literal = "%aa%aa%aa%", .exact_offsets = {4}},
        kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "OverlapTwoAb",
                                   dataset,
                                   {.op = PatternOp::Match,
                                    .literal = "%ab%ab%",
                                    .exact_offsets = {7, 8, 15, 16}},
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "OverlapThreeAb",
                                   dataset,
                                   {.op = PatternOp::Match,
                                    .literal = "%ab%ab%ab%",
                                    .exact_offsets = {8, 16}},
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "OverlapAaPrefixPostfix",
                                   dataset,
                                   {.op = PatternOp::Match,
                                    .literal = "aa%aa",
                                    .exact_offsets = {2, 3, 4}},
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "OverlapAbPrefixPostfix",
                                   dataset,
                                   {.op = PatternOp::Match,
                                    .literal = "ab%ab",
                                    .exact_offsets = {7, 8, 15, 16}},
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "OverlapTwoAbc",
                                   dataset,
                                   {.op = PatternOp::Match,
                                    .literal = "%abc%abc%",
                                    .exact_offsets = {15, 16}});
}

void
AddUtf8AndEscapeCases(IndexTestCases& cases) {
    constexpr auto utf8 = "NgramUtf8";
    AddNgramCase<std::string_view>(cases,
                                   "Utf8CafePrefix",
                                   utf8,
                                   {
                                       .op = PatternOp::PrefixMatch,
                                       .literal = "café",
                                       .exact_offsets = {0, 6},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "Utf8CafeInner",
                                   utf8,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "café",
                                       .exact_offsets = {0, 1, 6},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "Utf8ChinesePrefix",
                                   utf8,
                                   {
                                       .op = PatternOp::PrefixMatch,
                                       .literal = "你好",
                                       .exact_offsets = {2, 7},
                                   },
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "Utf8ChineseInner",
                                   utf8,
                                   {
                                       .op = PatternOp::InnerMatch,
                                       .literal = "你好",
                                       .exact_offsets = {2, 3, 7},
                                   },
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "Utf8ChinesePostfix",
                                   utf8,
                                   {
                                       .op = PatternOp::PostfixMatch,
                                       .literal = "你好",
                                       .exact_offsets = {7},
                                   },
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "Utf8CafeLike",
                                   utf8,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%café%",
                                       .exact_offsets = {0, 1, 6},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "Utf8ChineseLike",
                                   utf8,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%你好%",
                                       .exact_offsets = {2, 3, 7},
                                   },
                                   kMinGram2);
    AddNgramCase<std::string_view>(cases,
                                   "Utf8RepeatedCafe",
                                   utf8,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%café%café%",
                                       .exact_offsets = {6},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "Utf8RepeatedChinese",
                                   utf8,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%你好%你好%",
                                       .exact_offsets = {7},
                                   },
                                   kMinGram2);
    AddNgramCase<std::string_view>(
        cases,
        "Utf8ChineseMatchDeclinesMin3",
        utf8,
        {.op = PatternOp::Match, .literal = "%你好%", .can_handle = false},
        kMinGram3);
    AddNgramCase<std::string_view>(
        cases,
        "Utf8ChineseRegexDeclinesMin3",
        utf8,
        {.op = PatternOp::RegexMatch, .literal = "你好", .can_handle = false},
        kMinGram3);
    AddNgramCase<std::string_view>(
        cases,
        "Utf8ChinesePrefixDeclinesMin3",
        utf8,
        {.op = PatternOp::PrefixMatch, .literal = "你好", .can_handle = false},
        kMinGram3);
    AddNgramCase<std::string_view>(
        cases,
        "Utf8ChinesePostfixDeclinesMin3",
        utf8,
        {.op = PatternOp::PostfixMatch, .literal = "你好", .can_handle = false},
        kMinGram3);

    constexpr auto escapes = "NgramEscapes";
    AddNgramCase<std::string_view>(cases,
                                   "EscapedPercent",
                                   escapes,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%100\\%%",
                                       .exact_offsets = {0},
                                   });
    AddNgramCase<std::string_view>(cases,
                                   "EscapedUnderscore",
                                   escapes,
                                   {
                                       .op = PatternOp::Match,
                                       .literal = "%file\\_%",
                                       .exact_offsets = {2},
                                   });
    AddNgramCase<std::string_view>(
        cases,
        "DeclineSingleEscapedBackslash",
        escapes,
        {.op = PatternOp::Match, .literal = "%\\\\%", .can_handle = false});
}

void
AddJsonProjectedCases(IndexTestCases& cases) {
    constexpr auto dataset = "NgramJsonProjected";
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonMissingLiteral",
                                      dataset,
                                      {
                                          .op = PatternOp::InnerMatch,
                                          .literal = "nothing",
                                      });
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonInnerIl",
                                      dataset,
                                      {
                                          .op = PatternOp::InnerMatch,
                                          .literal = "il",
                                          .exact_offsets = {2, 3, 7, 9},
                                      });
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonInnerLliz",
                                      dataset,
                                      {
                                          .op = PatternOp::InnerMatch,
                                          .literal = "lliz",
                                          .exact_offsets = {3, 9},
                                      });
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonPrefixZi",
                                      dataset,
                                      {
                                          .op = PatternOp::PrefixMatch,
                                          .literal = "Zi",
                                          .exact_offsets = {3, 9},
                                      });
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonPrefixZilliz",
                                      dataset,
                                      {
                                          .op = PatternOp::PrefixMatch,
                                          .literal = "Zilliz",
                                          .exact_offsets = {3, 9},
                                      });
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonPostfixDe",
                                      dataset,
                                      {
                                          .op = PatternOp::PostfixMatch,
                                          .literal = "de",
                                          .exact_offsets = {4, 5},
                                      });
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonPostfixNode",
                                      dataset,
                                      {
                                          .op = PatternOp::PostfixMatch,
                                          .literal = "Node",
                                          .exact_offsets = {4, 5},
                                      });
    AddNgramCase<JsonProjectedString>(cases,
                                      "JsonLikeQueryNode",
                                      dataset,
                                      {
                                          .op = PatternOp::Match,
                                          .literal = "%ery%ode%",
                                          .exact_offsets = {4},
                                      });
}

const IndexTestCases&
NgramCases() {
    static const auto cases = [] {
        IndexTestCases cases;
        AddCanHandleCases(cases);
        AddWikiCases(cases);
        AddCoreCandidateCases(cases);
        AddMatcherAgreementCases(cases);
        AddOverlapCases(cases);
        AddUtf8AndEscapeCases(cases);
        AddJsonProjectedCases(cases);
        return cases;
    }();
    return cases;
}

class NgramReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(NgramReaderTest, PreservesRequiredCandidateSemantics) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(NgramBackends,
                         NgramReaderTest,
                         ::testing::ValuesIn(NgramCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
