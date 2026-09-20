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

#include <gtest/gtest.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "index/contracts/query/IPatternMatchReader.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
namespace {

struct PatternQuery {
    using ValueType = std::string_view;
    using Reader = IPatternMatchReader;
    static constexpr auto kCapability = &ReaderCaps::pattern_match;

    struct Args {
        PatternOp op;
        std::string pattern;
    };

    static PatternQueryPolicy
    QueryPolicy(const ReaderBackend& backend, const Args& args) {
        return backend.PatternPolicy(args.op);
    }

    static bool
    ShouldUse(const Reader& reader, const Args& args) {
        return reader.ShouldUseForOp(args.op, args.pattern);
    }

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        return reader.PatternMatch(args.pattern, args.op);
    }

    static TargetBitmap
    Oracle(const ScalarTestData<ValueType>& data, const Args& args) {
        TargetBitmap expected(data.values.size(), false);
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (!data.validity[i]) {
                continue;
            }
            bool matches = false;
            switch (args.op) {
                case PatternOp::PrefixMatch:
                    matches = data.values[i].starts_with(args.pattern);
                    break;
                case PatternOp::PostfixMatch:
                    matches = data.values[i].ends_with(args.pattern);
                    break;
                case PatternOp::InnerMatch:
                    matches = data.values[i].find(args.pattern) !=
                              std::string_view::npos;
                    break;
                case PatternOp::Match:
                case PatternOp::RegexMatch:
                    throw std::logic_error(
                        "LIKE and regex cases require manual expected offsets");
            }
            if (matches) {
                expected.set(i);
            }
        }
        return expected;
    }
};

void
AddOracleCase(IndexTestCases& cases,
              std::string name,
              std::string dataset,
              PatternOp op,
              std::string pattern) {
    cases.Add(IndexTestCase<std::string_view>{
        .name = std::move(name),
        .dataset = std::move(dataset),
        .body =
            Query<PatternQuery>{
                .args = {.op = op, .pattern = std::move(pattern)},
            },
    });
}

void
AddManualCase(IndexTestCases& cases,
              std::string name,
              std::string dataset,
              PatternOp op,
              std::string pattern,
              std::vector<size_t> offsets) {
    cases.Add(IndexTestCase<std::string_view>{
        .name = std::move(name),
        .dataset = std::move(dataset),
        .body =
            Query<PatternQuery>{
                .args = {.op = op, .pattern = std::move(pattern)},
                .expected = ManualHits(std::move(offsets)),
            },
    });
}

auto
ValidRows() {
    return [](const ScalarTestData<std::string_view>& data,
              const PatternQuery::Args&) {
        TargetBitmap result(data.values.size(), false);
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (data.validity[i]) {
                result.set(i);
            }
        }
        return result;
    };
}

auto
ValidNonEmptyRows() {
    return [](const ScalarTestData<std::string_view>& data,
              const PatternQuery::Args&) {
        TargetBitmap result(data.values.size(), false);
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (data.validity[i] && !data.values[i].empty()) {
                result.set(i);
            }
        }
        return result;
    };
}

void
AddLiteralCases(IndexTestCases& cases) {
    constexpr auto dataset = "PatternStringsNullable";

    AddOracleCase(cases, "PrefixBasic", dataset, PatternOp::PrefixMatch, "app");
    AddOracleCase(cases, "PrefixBan", dataset, PatternOp::PrefixMatch, "ban");
    AddOracleCase(cases, "PrefixCat", dataset, PatternOp::PrefixMatch, "cat");
    AddOracleCase(cases, "PrefixDog", dataset, PatternOp::PrefixMatch, "dog");
    AddOracleCase(
        cases, "PrefixHello", dataset, PatternOp::PrefixMatch, "hello");
    AddOracleCase(cases, "PrefixTest", dataset, PatternOp::PrefixMatch, "test");
    AddOracleCase(
        cases, "PrefixWorld", dataset, PatternOp::PrefixMatch, "world");
    AddOracleCase(
        cases, "PrefixMissing", dataset, PatternOp::PrefixMatch, "missing");
    AddOracleCase(cases, "PrefixEmpty", dataset, PatternOp::PrefixMatch, "");
    AddOracleCase(
        cases, "PrefixLiteralPercent", dataset, PatternOp::PrefixMatch, "%");
    AddOracleCase(
        cases, "PrefixLiteralUnderscore", dataset, PatternOp::PrefixMatch, "_");
    AddOracleCase(cases,
                  "PrefixLiteralBackslash",
                  dataset,
                  PatternOp::PrefixMatch,
                  "path\\");
    AddOracleCase(
        cases, "PrefixUnicode", dataset, PatternOp::PrefixMatch, "你好");
    AddOracleCase(cases,
                  "PrefixLong",
                  dataset,
                  PatternOp::PrefixMatch,
                  std::string(200, 'a'));

    AddOracleCase(
        cases, "PostfixBasic", dataset, PatternOp::PostfixMatch, "world");
    AddOracleCase(cases, "PostfixE", dataset, PatternOp::PostfixMatch, "e");
    AddOracleCase(
        cases, "PostfixPeace", dataset, PatternOp::PostfixMatch, "peace");
    AddOracleCase(
        cases, "PostfixHello", dataset, PatternOp::PostfixMatch, "hello");
    AddOracleCase(
        cases, "PostfixTest", dataset, PatternOp::PostfixMatch, "test");
    AddOracleCase(
        cases, "PostfixMissing", dataset, PatternOp::PostfixMatch, "missing");
    AddOracleCase(cases, "PostfixEmpty", dataset, PatternOp::PostfixMatch, "");
    AddOracleCase(
        cases, "PostfixLiteralPercent", dataset, PatternOp::PostfixMatch, "%a");
    AddOracleCase(cases,
                  "PostfixLiteralUnderscore",
                  dataset,
                  PatternOp::PostfixMatch,
                  "_");
    AddOracleCase(cases,
                  "PostfixLiteralBackslash",
                  dataset,
                  PatternOp::PostfixMatch,
                  "\\file");
    AddOracleCase(
        cases, "PostfixUnicode", dataset, PatternOp::PostfixMatch, "世界");
    AddOracleCase(cases,
                  "PostfixLong",
                  dataset,
                  PatternOp::PostfixMatch,
                  std::string(200, 'a'));

    AddOracleCase(cases, "InnerBasic", dataset, PatternOp::InnerMatch, "world");
    AddOracleCase(cases, "InnerPp", dataset, PatternOp::InnerMatch, "pp");
    AddOracleCase(cases, "InnerAn", dataset, PatternOp::InnerMatch, "an");
    AddOracleCase(cases, "InnerEllo", dataset, PatternOp::InnerMatch, "ello");
    AddOracleCase(cases, "InnerHello", dataset, PatternOp::InnerMatch, "hello");
    AddOracleCase(cases, "InnerTest", dataset, PatternOp::InnerMatch, "test");
    AddOracleCase(
        cases, "InnerMissing", dataset, PatternOp::InnerMatch, "missing");
    AddOracleCase(cases, "InnerEmpty", dataset, PatternOp::InnerMatch, "");
    AddOracleCase(
        cases, "InnerLiteralPercent", dataset, PatternOp::InnerMatch, "%");
    AddOracleCase(
        cases, "InnerLiteralUnderscore", dataset, PatternOp::InnerMatch, "_");
    AddOracleCase(cases,
                  "InnerLiteralBackslash",
                  dataset,
                  PatternOp::InnerMatch,
                  "\\to\\");
    AddOracleCase(
        cases, "InnerUnicode", dataset, PatternOp::InnerMatch, "你好");
    AddOracleCase(cases,
                  "InnerRepeatedOccurrences",
                  dataset,
                  PatternOp::InnerMatch,
                  "ab");
    AddOracleCase(cases,
                  "InnerLong",
                  dataset,
                  PatternOp::InnerMatch,
                  std::string(200, 'a'));

    for (const auto op : {PatternOp::PrefixMatch,
                          PatternOp::PostfixMatch,
                          PatternOp::InnerMatch}) {
        const auto name = op == PatternOp::PrefixMatch    ? "AllNullPrefix"
                          : op == PatternOp::PostfixMatch ? "AllNullPostfix"
                                                          : "AllNullInner";
        AddOracleCase(cases, name, "PatternAllNull", op, "");
    }
}

void
AddLikeCases(IndexTestCases& cases) {
    constexpr auto dataset = "PatternStringsNullable";

    AddManualCase(cases, "LikeExact", dataset, PatternOp::Match, "hello", {14});
    AddManualCase(cases,
                  "LikeNumericPrefix",
                  dataset,
                  PatternOp::Match,
                  "1%",
                  {27, 28, 30, 31, 32, 33, 59});
    AddManualCase(cases,
                  "LikePrefix",
                  dataset,
                  PatternOp::Match,
                  "hello%",
                  {11, 14, 37, 65, 71, 72, 88});
    AddManualCase(cases,
                  "LikeSuffix",
                  dataset,
                  PatternOp::Match,
                  "%world",
                  {11, 12, 15, 37, 65});
    AddManualCase(cases,
                  "LikeInner",
                  dataset,
                  PatternOp::Match,
                  "%llo%",
                  {11, 14, 16, 37, 50, 65, 66, 71, 72, 88});
    AddManualCase(
        cases, "LikeSingleWildcard", dataset, PatternOp::Match, "h_llo", {14});
    AddManualCase(cases, "LikeGap", dataset, PatternOp::Match, "h%o", {14});

    cases.Add(IndexTestCase<std::string_view>{
        .name = "LikePercentMatchesAllValid",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::Match, .pattern = "%"},
                .expected = ValidRows(),
            },
    });
    AddManualCase(cases,
                  "LikeTestPrefix",
                  dataset,
                  PatternOp::Match,
                  "test%",
                  {61, 62, 63});
    AddManualCase(
        cases, "LikeIngSuffix", dataset, PatternOp::Match, "%ing", {62, 64});
    AddManualCase(cases,
                  "LikeEstInner",
                  dataset,
                  PatternOp::Match,
                  "%est%",
                  {38, 61, 62, 63, 64});

    AddManualCase(cases,
                  "LikeAppPrefixDuplicates",
                  dataset,
                  PatternOp::Match,
                  "app%",
                  {0, 1, 2, 8});
    AddManualCase(cases,
                  "LikeAppPrefixAndSuffix",
                  dataset,
                  PatternOp::Match,
                  "app%ion",
                  {1, 8});
    AddManualCase(
        cases, "LikeBananaInner", dataset, PatternOp::Match, "%ana%", {3});
    AddManualCase(
        cases, "LikeCatPrefix", dataset, PatternOp::Match, "cat%", {5, 6});
    AddManualCase(cases,
                  "LikeHelloSuffix",
                  dataset,
                  PatternOp::Match,
                  "%hello",
                  {14, 16, 66});
    AddManualCase(cases,
                  "LikeWorldInner",
                  dataset,
                  PatternOp::Match,
                  "%world%",
                  {11, 12, 13, 15, 37, 65, 66, 67});

    AddManualCase(cases,
                  "LikeUnderscoreExactLength",
                  dataset,
                  PatternOp::Match,
                  "a_c",
                  {21, 22, 23});
    AddManualCase(cases,
                  "LikeUnderscorePrefix",
                  dataset,
                  PatternOp::Match,
                  "a_c%",
                  {21, 22, 23, 24, 81, 82, 83, 102, 103});
    AddManualCase(cases,
                  "LikeEscapedPercentExact",
                  dataset,
                  PatternOp::Match,
                  "100\\%",
                  {27});
    AddManualCase(cases,
                  "LikeEscapedPercentInner",
                  dataset,
                  PatternOp::Match,
                  "%\\%%",
                  {19, 27, 29, 30, 31, 32, 70, 71, 76});
    AddManualCase(cases,
                  "LikeEscapedUnderscoreExact",
                  dataset,
                  PatternOp::Match,
                  "a\\_b",
                  {17});
    AddManualCase(cases,
                  "LikeEscapedPercentInfix",
                  dataset,
                  PatternOp::Match,
                  "a\\%b",
                  {19});
    AddManualCase(cases,
                  "LikeComplexEscapesExact",
                  dataset,
                  PatternOp::Match,
                  "10\\%\\_off",
                  {30});
    AddManualCase(cases,
                  "LikeMixedEscapeAndWildcard",
                  dataset,
                  PatternOp::Match,
                  "10\\%_off",
                  {30, 31, 32});
    AddManualCase(cases,
                  "LikeOrderedSegments",
                  dataset,
                  PatternOp::Match,
                  "a%b%c",
                  {21, 26, 84, 85, 100, 101});

    AddManualCase(cases,
                  "LikeOverlapTwoSameSegments",
                  dataset,
                  PatternOp::Match,
                  "%aa%aa%",
                  {45, 47, 48, 49});
    AddManualCase(cases,
                  "LikeOverlapThreeSameSegments",
                  dataset,
                  PatternOp::Match,
                  "%aa%aa%aa%",
                  {47, 48, 49});
    AddManualCase(cases,
                  "LikeOverlapDifferentSegments",
                  dataset,
                  PatternOp::Match,
                  "%ab%ba%",
                  {46, 85});
    AddManualCase(cases,
                  "LikeRepeatedAbTwice",
                  dataset,
                  PatternOp::Match,
                  "%ab%ab%",
                  {46, 84, 85});
    AddManualCase(cases,
                  "LikeRepeatedAbThreeTimes",
                  dataset,
                  PatternOp::Match,
                  "%ab%ab%ab%",
                  {46, 85});
    AddManualCase(cases,
                  "LikeRepeatedSuffix",
                  dataset,
                  PatternOp::Match,
                  "a%aa",
                  {45, 47, 48, 49});
    AddManualCase(cases,
                  "LikeRepeatedPrefix",
                  dataset,
                  PatternOp::Match,
                  "aa%a",
                  {45, 47, 48, 49});
    AddManualCase(cases,
                  "LikeTwoSingleSegments",
                  dataset,
                  PatternOp::Match,
                  "%a%a%",
                  {1, 3, 8, 45, 46, 47, 48, 49, 84, 85});
    AddManualCase(cases,
                  "LikeThreeSingleSegments",
                  dataset,
                  PatternOp::Match,
                  "%a%a%a%",
                  {3, 45, 46, 47, 48, 49, 85});
    AddManualCase(cases,
                  "LikeAnchoredRepeatedLetter",
                  dataset,
                  PatternOp::Match,
                  "a%a",
                  {45, 47, 48, 49});

    AddManualCase(
        cases, "LikeUnicodeWildcard", dataset, PatternOp::Match, "caf_", {35});
    AddManualCase(
        cases, "LikeUnicodeInner", dataset, PatternOp::Match, "%你%", {36, 37});
    AddManualCase(cases,
                  "LikeUnicodeSingleCodepoint",
                  dataset,
                  PatternOp::Match,
                  "a_b",
                  {17, 18, 19, 39, 40, 41, 93, 98, 99});
    AddManualCase(
        cases, "LikeEmojiInner", dataset, PatternOp::Match, "%😀%", {38, 88});
    AddManualCase(
        cases, "LikeUnicodePrefix", dataset, PatternOp::Match, "你%", {36});
    AddManualCase(
        cases, "LikeUnicodeSuffix", dataset, PatternOp::Match, "%好", {74});
    AddManualCase(cases,
                  "LikeCjkSingleCodepoint",
                  dataset,
                  PatternOp::Match,
                  "你_世界",
                  {36});
    AddManualCase(cases,
                  "LikeEmojiSingleCodepoint",
                  dataset,
                  PatternOp::Match,
                  "emoji_test",
                  {38});
    AddManualCase(cases,
                  "LikeUtf8BoundaryNoFalsePositive",
                  dataset,
                  PatternOp::Match,
                  "%©%",
                  {});

    AddManualCase(cases,
                  "LikeLiteralPercentSuffix",
                  dataset,
                  PatternOp::Match,
                  "%\\%",
                  {27, 76});
    AddManualCase(cases,
                  "LikeLiteralPercentPrefix",
                  dataset,
                  PatternOp::Match,
                  "\\%%",
                  {70, 76});
    AddManualCase(cases,
                  "LikeEscapedUnderscorePrefix",
                  dataset,
                  PatternOp::Match,
                  "file\\_name%",
                  {75});
    AddManualCase(cases,
                  "LikeLiteralBackslashInner",
                  dataset,
                  PatternOp::Match,
                  "%\\\\%",
                  {34, 98});
    AddManualCase(cases,
                  "LikeEscapedPercentBothEnds",
                  dataset,
                  PatternOp::Match,
                  "\\%percent\\%",
                  {76});
    AddManualCase(cases,
                  "LikeEscapedUnderscoreBothEnds",
                  dataset,
                  PatternOp::Match,
                  "\\_underscore\\_",
                  {77});
    AddManualCase(cases,
                  "LikeRegexDotIsLiteral",
                  dataset,
                  PatternOp::Match,
                  "file.txt",
                  {53});
    AddManualCase(cases,
                  "LikeRegexMetacharactersAreLiteral",
                  dataset,
                  PatternOp::Match,
                  "()[]{}%",
                  {55});

    AddManualCase(
        cases, "LikeOneCodepoint", dataset, PatternOp::Match, "_", {73});
    AddManualCase(cases,
                  "LikeTwoCodepoints",
                  dataset,
                  PatternOp::Match,
                  "__",
                  {20, 25, 70, 74});
    AddManualCase(
        cases,
        "LikeThreeCodepoints",
        dataset,
        PatternOp::Match,
        "___",
        {5, 7, 17, 18, 19, 21, 22, 23, 39, 40, 41, 43, 60, 80, 93, 98, 99});
    cases.Add(IndexTestCase<std::string_view>{
        .name = "LikeRepeatedPercentMatchesAllValid",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::Match, .pattern = "%%"},
                .expected = ValidRows(),
            },
    });
    for (const auto& [name, pattern] : {
             std::pair{"LikeLeadingSingleWildcard", "_%"},
             std::pair{"LikeTrailingSingleWildcard", "%_"},
         }) {
        cases.Add(IndexTestCase<std::string_view>{
            .name = name,
            .dataset = dataset,
            .body =
                Query<PatternQuery>{
                    .args = {.op = PatternOp::Match, .pattern = pattern},
                    .expected = ValidNonEmptyRows(),
                },
        });
    }
    AddManualCase(
        cases, "LikeEmptyPattern", dataset, PatternOp::Match, "", {9});

    for (const auto& [name, pattern] : {
             std::pair{"LikeTrailingBackslashRejected", "abc\\"},
             std::pair{"LikeOnlyBackslashRejected", "\\"},
             std::pair{"LikeWildcardTrailingBackslashRejected", "%\\"},
         }) {
        cases.Add(IndexTestCase<std::string_view>{
            .name = name,
            .dataset = dataset,
            .body =
                Query<PatternQuery>{
                    .args = {.op = PatternOp::Match, .pattern = pattern},
                    .expected_error = ErrorCode::ExprInvalid,
                },
        });
    }

    AddManualCase(cases,
                  "LikeSpecialBytesPrefixSuffix",
                  dataset,
                  PatternOp::Match,
                  "hello%world",
                  {11, 37, 65});
    AddManualCase(cases,
                  "LikeSpecialBytesGap",
                  dataset,
                  PatternOp::Match,
                  "a%b",
                  {17, 18, 19, 20, 39, 40, 41, 42, 46, 93, 98, 99});
    AddManualCase(
        cases, "LikeCrLfTwoWildcards", dataset, PatternOp::Match, "a__b", {42});

    cases.Add(IndexTestCase<std::string_view>{
        .name = "AllNullLike",
        .dataset = "PatternAllNull",
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::Match, .pattern = "%"},
                .expected = ManualHits({}),
            },
    });
}

void
AddEmbeddedNulCases(IndexTestCases& cases) {
    constexpr auto dataset = "PatternBinaryNullable";

    AddOracleCase(cases,
                  "NulPrefixLiteral",
                  dataset,
                  PatternOp::PrefixMatch,
                  std::string("a\0", 2));
    AddOracleCase(cases,
                  "NulPostfixLiteral",
                  dataset,
                  PatternOp::PostfixMatch,
                  std::string("\0b", 2));
    AddOracleCase(cases,
                  "NulInnerLiteral",
                  dataset,
                  PatternOp::InnerMatch,
                  std::string("\0", 1));
    AddOracleCase(cases,
                  "NulInnerMultiByteLiteral",
                  dataset,
                  PatternOp::InnerMatch,
                  std::string("a\0b", 3));

    cases.Add(IndexTestCase<std::string_view>{
        .name = "NulLikeAll",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::Match, .pattern = "%"},
                .expected = ValidRows(),
            },
    });
    AddManualCase(
        cases, "NulLikeGap", dataset, PatternOp::Match, "a%b", {0, 3, 6});
    AddManualCase(
        cases, "NulLikePrefix", dataset, PatternOp::Match, "a%", {0, 3, 6, 9});
    AddManualCase(
        cases, "NulLikePostfix", dataset, PatternOp::Match, "%b", {0, 3, 6});
    AddManualCase(
        cases, "NulLikeToSuffix", dataset, PatternOp::Match, "a%c", {9});
    AddManualCase(cases,
                  "NulLikePrefixPastNul",
                  dataset,
                  PatternOp::Match,
                  "hello%",
                  {1, 5});
    AddManualCase(cases,
                  "NulLikePostfixPastNul",
                  dataset,
                  PatternOp::Match,
                  "%hello",
                  {1, 4});
    AddManualCase(
        cases, "NulLikeSingleWildcard", dataset, PatternOp::Match, "a_b", {3});
    AddManualCase(cases,
                  "NulLikeLeadingWildcard",
                  dataset,
                  PatternOp::Match,
                  "_hello",
                  {4});
    AddManualCase(cases,
                  "NulLikeTrailingWildcard",
                  dataset,
                  PatternOp::Match,
                  "hello_",
                  {5});
    AddManualCase(
        cases, "NulLikeTwoWildcards", dataset, PatternOp::Match, "a__b", {6});
    AddManualCase(
        cases, "NulLikeOneCodepoint", dataset, PatternOp::Match, "_", {7});
    AddManualCase(
        cases, "NulLikeTwoCodepoints", dataset, PatternOp::Match, "__", {0, 8});
    AddManualCase(
        cases, "NulLikeExactAscii", dataset, PatternOp::Match, "ab", {0});
    AddManualCase(cases,
                  "NulLikeExactLiteral",
                  dataset,
                  PatternOp::Match,
                  std::string("a\0b", 3),
                  {3});
    AddManualCase(cases,
                  "NulLikeLiteralThenPercent",
                  dataset,
                  PatternOp::Match,
                  std::string("a\0%", 3),
                  {3, 6, 9});
    AddManualCase(cases,
                  "NulLikePercentThenLiteral",
                  dataset,
                  PatternOp::Match,
                  std::string("%\0b", 3),
                  {3, 6});
    AddManualCase(cases,
                  "NulLikeLiteralPrefix",
                  dataset,
                  PatternOp::Match,
                  std::string("\0%", 2),
                  {4, 7, 8});
    AddManualCase(cases,
                  "NulLikeLiteralPostfix",
                  dataset,
                  PatternOp::Match,
                  std::string("%\0", 2),
                  {5, 7, 8});
    AddManualCase(cases,
                  "NulLikeLiteralInner",
                  dataset,
                  PatternOp::Match,
                  std::string("%\0%", 3),
                  {3, 4, 5, 6, 7, 8, 9, 10});
    AddManualCase(cases,
                  "NulLikeLiteralAndWildcard",
                  dataset,
                  PatternOp::Match,
                  std::string("a\0_", 3),
                  {3});

    AddManualCase(cases,
                  "NulRegexLiteralPartial",
                  dataset,
                  PatternOp::RegexMatch,
                  std::string("a\0b", 3),
                  {3, 9});
    AddManualCase(cases,
                  "NulRegexLiteralAnchored",
                  dataset,
                  PatternOp::RegexMatch,
                  std::string("^a\0b$", 5),
                  {3});
}

void
AddRegexCases(IndexTestCases& cases) {
    constexpr auto dataset = "PatternStringsNullable";

    AddManualCase(cases,
                  "RegexLiteralPartial",
                  dataset,
                  PatternOp::RegexMatch,
                  "abc",
                  {21, 24, 58, 59, 81, 82, 83, 84, 85, 94, 102, 103});
    AddManualCase(cases,
                  "RegexNumericPrefix",
                  dataset,
                  PatternOp::RegexMatch,
                  "^1.*",
                  {27, 28, 30, 31, 32, 33, 59});
    AddManualCase(cases,
                  "RegexNullPayloadExcluded",
                  dataset,
                  PatternOp::RegexMatch,
                  "apple",
                  {0});
    AddManualCase(cases,
                  "RegexStartAnchor",
                  dataset,
                  PatternOp::RegexMatch,
                  "^hello",
                  {11, 14, 37, 65, 71, 72, 88});
    AddManualCase(cases,
                  "RegexEndAnchor",
                  dataset,
                  PatternOp::RegexMatch,
                  "world$",
                  {11, 12, 15, 37, 65});
    AddManualCase(cases,
                  "RegexBothAnchors",
                  dataset,
                  PatternOp::RegexMatch,
                  "^exact$",
                  {78});
    AddManualCase(cases,
                  "RegexCharacterClass",
                  dataset,
                  PatternOp::RegexMatch,
                  "[0-9]+",
                  {23, 27, 28, 29, 30, 31, 32, 33, 52, 59, 86, 87, 92});
    AddManualCase(
        cases,
        "RegexLowercaseStart",
        dataset,
        PatternOp::RegexMatch,
        "^[a-z]",
        {0,  1,  2,  3,  4,  5,  6,  7,  8,  11,  12,  13,  14,  15, 16, 17, 18,
         19, 20, 21, 22, 23, 24, 25, 26, 34, 35,  37,  38,  39,  40, 41, 42, 45,
         46, 47, 48, 49, 50, 51, 52, 53, 54, 56,  57,  58,  61,  62, 63, 64, 65,
         66, 67, 68, 69, 71, 72, 75, 78, 79, 80,  81,  82,  83,  84, 85, 88, 89,
         90, 91, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104});
    AddManualCase(cases,
                  "RegexDotIncludesNewline",
                  dataset,
                  PatternOp::RegexMatch,
                  "^a.b$",
                  {17, 18, 19, 39, 40, 41, 93, 98, 99});
    AddManualCase(cases,
                  "RegexDotPartial",
                  dataset,
                  PatternOp::RegexMatch,
                  "a.c",
                  {21, 22, 23, 24, 58, 59, 81, 82, 83, 84, 85, 94, 102, 103});
    cases.Add(IndexTestCase<std::string_view>{
        .name = "RegexEmptyMatchesAllValid",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = ""},
                .expected = ValidRows(),
            },
    });
    cases.Add(IndexTestCase<std::string_view>{
        .name = "RegexDotStarMatchesAllValid",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = ".*"},
                .expected = ValidRows(),
            },
    });
    cases.Add(IndexTestCase<std::string_view>{
        .name = "RegexLazyDotStarMatchesAllValid",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = ".*?"},
                .expected = ValidRows(),
            },
    });
    AddManualCase(cases,
                  "RegexDotNewlineDisabled",
                  dataset,
                  PatternOp::RegexMatch,
                  "(?-s)^a.b$",
                  {17, 18, 19, 39, 40, 93, 98, 99});
    AddManualCase(cases,
                  "RegexAlternation",
                  dataset,
                  PatternOp::RegexMatch,
                  "abc|fgk|xyz",
                  {21, 24, 58, 59, 80, 81, 82, 83, 84, 85, 94, 102, 103});
    cases.Add(IndexTestCase<std::string_view>{
        .name = "RegexEmptyAlternationBranch",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = "(abc|)"},
                .expected = ValidRows(),
            },
    });
    AddManualCase(cases,
                  "RegexOptionalGroup",
                  dataset,
                  PatternOp::RegexMatch,
                  "abc(de)?fg",
                  {81, 82});
    AddManualCase(cases,
                  "RegexRepeatedGroup",
                  dataset,
                  PatternOp::RegexMatch,
                  "(ab){2,3}c",
                  {84, 85});
    AddManualCase(cases,
                  "RegexNamedGroup",
                  dataset,
                  PatternOp::RegexMatch,
                  "(?P<num>\\d+)abc",
                  {59});
    AddManualCase(cases,
                  "RegexUnicodeYen",
                  dataset,
                  PatternOp::RegexMatch,
                  "\\x{00A5}\\d+",
                  {86});
    AddManualCase(cases,
                  "RegexUnicodeEmoji",
                  dataset,
                  PatternOp::RegexMatch,
                  ".+\\x{1F600}",
                  {38, 88});
    AddManualCase(
        cases, "RegexTabEscape", dataset, PatternOp::RegexMatch, "a\\tb", {40});
    AddManualCase(cases,
                  "RegexCarriageReturnEscape",
                  dataset,
                  PatternOp::RegexMatch,
                  "a\\rb",
                  {93});
    AddManualCase(
        cases,
        "RegexLazyQuantifier",
        dataset,
        PatternOp::RegexMatch,
        "a.+?b",
        {17, 18, 19, 26, 39, 40, 41, 42, 46, 84, 85, 93, 98, 99, 100, 101});
    AddManualCase(cases,
                  "RegexNestedGroups",
                  dataset,
                  PatternOp::RegexMatch,
                  "^x((a)(b(c)))y$",
                  {94});
    AddManualCase(cases,
                  "RegexEscapedDot",
                  dataset,
                  PatternOp::RegexMatch,
                  "file\\.txt",
                  {53});
    AddManualCase(cases,
                  "RegexOrderedLiterals",
                  dataset,
                  PatternOp::RegexMatch,
                  "error.*timeout",
                  {96});
    AddManualCase(cases,
                  "RegexEscapedBackslash",
                  dataset,
                  PatternOp::RegexMatch,
                  "a\\\\b",
                  {98});
    AddManualCase(cases,
                  "RegexEscapedDots",
                  dataset,
                  PatternOp::RegexMatch,
                  "a\\.b\\.c",
                  {100});
    AddManualCase(cases,
                  "RegexDotAcrossNewline",
                  dataset,
                  PatternOp::RegexMatch,
                  "abc.def",
                  {102, 103});
    AddManualCase(cases,
                  "RegexDotStarAcrossNewlines",
                  dataset,
                  PatternOp::RegexMatch,
                  "a.*z",
                  {104});
    AddManualCase(cases,
                  "RegexEscapedMetacharacters",
                  dataset,
                  PatternOp::RegexMatch,
                  "\\(\\)\\[\\]\\{\\}",
                  {55});
    AddManualCase(cases,
                  "RegexWordBoundary",
                  dataset,
                  PatternOp::RegexMatch,
                  "\\berror\\b",
                  {56, 96, 97});
    AddManualCase(cases,
                  "RegexCaseSensitive",
                  dataset,
                  PatternOp::RegexMatch,
                  "Hello",
                  {50});
    AddManualCase(cases,
                  "RegexCaseInsensitive",
                  dataset,
                  PatternOp::RegexMatch,
                  "(?i)hello",
                  {11, 14, 16, 37, 50, 51, 65, 66, 71, 72, 88});
    AddManualCase(cases,
                  "RegexLongLiteral",
                  dataset,
                  PatternOp::RegexMatch,
                  std::string(200, 'a'),
                  {47, 48});
    AddManualCase(cases,
                  "RegexMixedLiteralsAndClass",
                  dataset,
                  PatternOp::RegexMatch,
                  "user_[0-9]+@gmail\\.com",
                  {52});
    AddManualCase(cases,
                  "RegexAlternationGroup",
                  dataset,
                  PatternOp::RegexMatch,
                  "(https?|ftp)://",
                  {90, 91});
    AddManualCase(cases,
                  "RegexUnicodeClass",
                  dataset,
                  PatternOp::RegexMatch,
                  "\\p{Han}+[0-9]+",
                  {92});

    cases.Add(IndexTestCase<std::string_view>{
        .name = "RegexBackreferenceRejected",
        .dataset = dataset,
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = "(a)\\1"},
                .expected_error = ErrorCode::InvalidParameter,
            },
    });
    cases.Add(IndexTestCase<std::string_view>{
        .name = "AllNullRegex",
        .dataset = "PatternAllNull",
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = ".*"},
                .expected = ManualHits({}),
            },
    });
}

void
AddHybridRoutingCases(IndexTestCases& cases) {
    for (const auto& [name, op, pattern] : {
             std::tuple{"HybridBitmapPostfix",
                        PatternOp::PostfixMatch,
                        std::string("b")},
             std::tuple{"HybridBitmapInner",
                        PatternOp::InnerMatch,
                        std::string("hello")},
         }) {
        cases.Add(IndexTestCase<std::string_view>{
            .name = name,
            .dataset = "PatternBinaryNullable",
            .families = {"hybrid"},
            .body =
                Query<PatternQuery>{
                    .args = {.op = op, .pattern = pattern},
                    .expected_should_use = true,
                },
        });
    }
    cases.Add(IndexTestCase<std::string_view>{
        .name = "HybridBitmapRegex",
        .dataset = "PatternBinaryNullable",
        .families = {"hybrid"},
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = "hello"},
                .expected_should_use = true,
                .expected = ManualHits({1, 4, 5}),
            },
    });

    for (const auto& [name, op] : {
             std::pair{"HybridInvertedPostfix", PatternOp::PostfixMatch},
             std::pair{"HybridInvertedInner", PatternOp::InnerMatch},
         }) {
        cases.Add(IndexTestCase<std::string_view>{
            .name = name,
            .dataset = "PatternStringsNullable",
            .families = {"hybrid"},
            .body =
                Query<PatternQuery>{
                    .args = {.op = op, .pattern = "world"},
                    .expected_should_use = false,
                },
        });
    }
    cases.Add(IndexTestCase<std::string_view>{
        .name = "HybridInvertedRegex",
        .dataset = "PatternStringsNullable",
        .families = {"hybrid"},
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::RegexMatch, .pattern = "world"},
                .expected_should_use = false,
                .expected = ManualHits({11, 12, 13, 15, 37, 65, 66, 67}),
            },
    });
}

void
AddFmRoutingCases(IndexTestCases& cases) {
    constexpr auto dataset = "PatternSelective";

    for (const auto& [name, op, pattern, should_use] : {
             std::tuple{"FmRarePrefix", PatternOp::PrefixMatch, "QOP", true},
             std::tuple{
                 "FmRarePostfix", PatternOp::PostfixMatch, "ZEBRA", true},
             std::tuple{"FmRareInner", PatternOp::InnerMatch, "ZEBRA", true},
             std::tuple{
                 "FmRareInnerAtStart", PatternOp::InnerMatch, "QOP", true},
             std::tuple{"FmAbsentInner", PatternOp::InnerMatch, "QUOKKA", true},
             std::tuple{
                 "FmCommonInner", PatternOp::InnerMatch, "COMMON", false},
             std::tuple{"FmSingleByteInner", PatternOp::InnerMatch, "x", false},
             std::tuple{"FmEmptyPrefix", PatternOp::PrefixMatch, "", true},
         }) {
        cases.Add(IndexTestCase<std::string_view>{
            .name = name,
            .dataset = dataset,
            .families = {"fmindex"},
            .body =
                Query<PatternQuery>{
                    .args = {.op = op, .pattern = pattern},
                    .expected_should_use = should_use,
                },
        });
    }

    cases.Add(IndexTestCase<std::string_view>{
        .name = "FmZeroTokenAbsentInner",
        .dataset = "PatternAllNull",
        .families = {"fmindex"},
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::InnerMatch, .pattern = "absent"},
                .expected_should_use = true,
            },
    });
    cases.Add(IndexTestCase<std::string_view>{
        .name = "FmZeroTokenEmptyInner",
        .dataset = "PatternAllNull",
        .families = {"fmindex"},
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::InnerMatch, .pattern = ""},
                .expected_should_use = true,
            },
    });
}

void
AddRandomByteCases(IndexTestCases& cases) {
    constexpr auto dataset = "PatternRandomBytes";

    for (const auto& [name, op, pattern] : {
             std::tuple{"RandomBytePrefix",
                        PatternOp::PrefixMatch,
                        std::string("\0\1", 2)},
             std::tuple{"RandomBytePostfix",
                        PatternOp::PostfixMatch,
                        std::string("\2\3", 2)},
             std::tuple{"RandomByteInner",
                        PatternOp::InnerMatch,
                        std::string("\0\1\2", 3)},
             std::tuple{"RandomByteSingle",
                        PatternOp::InnerMatch,
                        std::string("\3", 1)},
         }) {
        cases.Add(IndexTestCase<std::string_view>{
            .name = name,
            .dataset = dataset,
            .families = {"fmindex"},
            .body =
                Query<PatternQuery>{
                    .args = {.op = op, .pattern = pattern},
                },
        });
    }
}

void
AddBackendLayoutCases(IndexTestCases& cases) {
    cases.Add(IndexTestCase<std::string_view>{
        .name = "BitmapRoaringMmapPrefix",
        .dataset = "PatternHighCardinality",
        .families = {"bitmap"},
        .body =
            Query<PatternQuery>{
                .args = {.op = PatternOp::PrefixMatch, .pattern = "key_00"},
            },
    });
}

void
AddAllValidProfileCases(IndexTestCases& cases) {
    constexpr auto dataset = "PredicateAllValid";

    AddOracleCase(
        cases, "AllValidPrefix", dataset, PatternOp::PrefixMatch, "a");
    AddOracleCase(
        cases, "AllValidPostfix", dataset, PatternOp::PostfixMatch, "b");
    AddOracleCase(cases, "AllValidInner", dataset, PatternOp::InnerMatch, "a");
    AddOracleCase(
        cases, "AllValidEmptyLiteral", dataset, PatternOp::PrefixMatch, "");
    AddManualCase(
        cases, "AllValidLike", dataset, PatternOp::Match, "a%", {1, 2, 3});
    AddManualCase(
        cases, "AllValidRegex", dataset, PatternOp::RegexMatch, "^ab$", {2});
}

const IndexTestCases&
PatternCases() {
    static const auto cases = [] {
        IndexTestCases cases;
        const std::string prefix(64, 'x');

        // Reuse the same nullable string dataset as In, with different queries.
        cases.Add(IndexTestCase<std::string_view>{
            .name = "RepeatedValues",
            .dataset = "RepeatedNullable",
            .body =
                Query<PatternQuery>{
                    .args = {.op = PatternOp::PrefixMatch,
                             .pattern = prefix + "1"},
                },
        });
        cases.Add(IndexTestCase<std::string_view>{
            .name = "MissingPrefix",
            .dataset = "RepeatedNullable",
            .body =
                Query<PatternQuery>{
                    .args = {.op = PatternOp::PrefixMatch,
                             .pattern = prefix + "9"},
                },
        });

        AddLiteralCases(cases);
        AddLikeCases(cases);
        AddEmbeddedNulCases(cases);
        AddRegexCases(cases);
        AddFmRoutingCases(cases);
        AddHybridRoutingCases(cases);
        AddRandomByteCases(cases);
        AddBackendLayoutCases(cases);
        AddAllValidProfileCases(cases);

        return cases;
    }();
    return cases;
}

class PatternMatchReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(PatternMatchReaderTest, MatchesExpectedOffsetsAndRouting) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarReaders,
                         PatternMatchReaderTest,
                         ::testing::ValuesIn(PatternCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
