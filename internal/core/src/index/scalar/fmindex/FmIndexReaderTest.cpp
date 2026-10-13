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

#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/RegexQuery.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

TEST(FmIndexReaderTest, AdvertisesSelectiveVarcharPatternPolicy) {
    for (const auto name : {"FmIndexVarchar",
                            "FmIndexVarcharMmap",
                            "FmIndexVarcharNonNull",
                            "FmIndexVarcharNonNullMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        const auto caps = backend.DeriveCaps();
        EXPECT_FALSE(caps.predicate) << name;
        EXPECT_TRUE(caps.pattern_match) << name;
        // General LIKE is served, but as a candidate superset the executor
        // must recheck (FmIndexReader::PatternMatchIsExact(Match) == false).
        EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
                  PatternQueryPolicy::CandidatesAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PrefixMatch),
                  PatternQueryPolicy::SelectiveAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PostfixMatch),
                  PatternQueryPolicy::SelectiveAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::InnerMatch),
                  PatternQueryPolicy::SelectiveAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::RegexMatch),
                  PatternQueryPolicy::CandidatesAndRun);
    }
}

IIndexReaderBasePtr
BuildFmReader(std::vector<std::string> values) {
    ScalarTestData<std::string_view> data(std::move(values));
    data.validity_present = false;
    const ScalarTestInput<std::string_view> input(data);
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto artifact =
        backend.Build(input.View(), {.row_count = data.values.size()});
    return backend.Open(std::move(artifact), {.row_count = data.values.size()});
}

TEST(FmIndexReaderTest, RepeatedOccurrencesMapToOneRow) {
    auto reader = BuildFmReader(
        {std::string(100, 'a'), "zzzz", "xxabxx", std::string(400, 'a')});
    ASSERT_NE(reader, nullptr);
    const auto* pattern =
        dynamic_cast<const IPatternMatchReader*>(reader.get());
    ASSERT_NE(pattern, nullptr);
    const auto hits = pattern->PatternMatch("a", PatternOp::InnerMatch);
    ASSERT_EQ(hits.size(), 4);
    EXPECT_EQ(hits.count(), 3);
    EXPECT_TRUE(hits[0]);
    EXPECT_FALSE(hits[1]);
    EXPECT_TRUE(hits[2]);
    EXPECT_TRUE(hits[3]);
}

TEST(FmIndexReaderTest, RandomBytesAgreeWithIndependentAnchoredOracle) {
    std::mt19937 rng(0xF3A1u);
    auto random_string = [&](size_t max_length, uint32_t alphabet) {
        const auto length = static_cast<size_t>(rng()) % (max_length + 1);
        std::string value;
        value.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            value.push_back(static_cast<char>(rng() % alphabet));
        }
        return value;
    };
    std::vector<std::string> rows;
    rows.reserve(300);
    for (size_t i = 0; i < 280; ++i) {
        rows.push_back(random_string(12, 4));
    }
    for (size_t i = 0; i < 20; ++i) {
        rows.push_back(random_string(20, 256));
    }
    auto reader = BuildFmReader(rows);
    ASSERT_NE(reader, nullptr);
    const auto* pattern =
        dynamic_cast<const IPatternMatchReader*>(reader.get());
    ASSERT_NE(pattern, nullptr);

    const auto check = [&](std::string_view needle, PatternOp op) {
        const auto result = pattern->PatternMatch(needle, op);
        ASSERT_EQ(result.size(), rows.size());
        for (size_t row = 0; row < rows.size(); ++row) {
            bool expected = false;
            switch (op) {
                case PatternOp::PrefixMatch:
                    expected = rows[row].starts_with(needle);
                    break;
                case PatternOp::PostfixMatch:
                    expected = rows[row].ends_with(needle);
                    break;
                case PatternOp::InnerMatch:
                    expected = rows[row].find(needle) != std::string::npos;
                    break;
                default:
                    FAIL() << "unexpected anchored operation";
            }
            EXPECT_EQ(static_cast<bool>(result[row]), expected)
                << "row=" << row << " pattern bytes=" << needle.size();
        }
    };
    for (size_t i = 0; i < 200; ++i) {
        const auto needle = random_string(3, 4);
        if (needle.empty()) {
            continue;
        }
        check(needle, PatternOp::PrefixMatch);
        check(needle, PatternOp::PostfixMatch);
        check(needle, PatternOp::InnerMatch);
    }
    for (size_t i = 0; i < 40; ++i) {
        const auto& row = rows[static_cast<size_t>(rng()) % rows.size()];
        if (row.empty()) {
            continue;
        }
        const auto start = static_cast<size_t>(rng()) % row.size();
        const auto length =
            1 + static_cast<size_t>(rng()) % (row.size() - start);
        check(std::string_view(row).substr(start, length),
              PatternOp::InnerMatch);
    }
}

TEST(FmIndexReaderTest, GeneralLikeCandidatesPreserveEveryExactMatch) {
    const std::vector<std::string> rows{
        "apple",
        "apply",
        "banana",
        "grape",
        "application",
        "app",
        "",
        "foo bar",
        "fooXbar",
        "café",
        "caf\xC3\xA9 extra",
        "你好世界",
        "hello你好world",
        "a_b",
        "a%b",
        "100%",
        std::string(200, 'z') + "NEEDLE" + std::string(200, 'z'),
        std::string(80, 'q')};
    auto reader = BuildFmReader(rows);
    ASSERT_NE(reader, nullptr);
    const auto* pattern =
        dynamic_cast<const IPatternMatchReader*>(reader.get());
    ASSERT_NE(pattern, nullptr);
    EXPECT_FALSE(pattern->PatternMatchIsExact(PatternOp::Match));

    const std::vector<std::string> patterns{
        "a%e",       "%foo%bar%", "foo%bar",     "%app%",       "app%",
        "%app",      "a_c",       "a_pple",      "%_%",         "%",
        "%%",        "_",         "a\\_b",       "%\\%%",       "100\\%",
        "%café%",    "%你好%",    "hello%world", "%NEEDLE%",    "%q%",
        "nope%nope", "",          "app",         "%zz%NEEDLE%", "application",
        "%X%"};

    for (const auto& query : patterns) {
        SCOPED_TRACE(query);
        LikePatternMatcher matcher(query);
        const auto candidates = pattern->PatternMatch(query, PatternOp::Match);
        ASSERT_EQ(candidates.size(), rows.size());
        std::vector<size_t> expected;
        std::vector<size_t> rechecked;
        for (size_t row = 0; row < rows.size(); ++row) {
            const bool exact = matcher(rows[row]);
            if (exact) {
                expected.push_back(row);
                EXPECT_TRUE(candidates[row]) << "missing row " << row;
            }
            if (candidates[row] && exact) {
                rechecked.push_back(row);
            }
        }
        EXPECT_EQ(rechecked, expected);
    }
}

std::shared_ptr<const IPatternMatchReader>
BuildFmPatternReader(const std::vector<std::string>& rows) {
    std::shared_ptr<IIndexReaderBase> reader(BuildFmReader(rows));
    return std::dynamic_pointer_cast<const IPatternMatchReader>(
        std::move(reader));
}

TEST(FmIndexReaderTest, RegexCandidatesAndRecheckEqualCanonicalScan) {
    const std::vector<std::string> rows{
        "",
        "foo",
        "foobar",
        "foo\nbar",
        "barfoo",
        "FOO",
        "food",
        "fool",
        "Aa.",
        "41",
        "a|b",
        "a\\b",
        "ab.*",
        "aab",
        "b",
        "ééb",
        "你好世界",
        std::string("a\0b", 3),
        std::string("a\xff"
                    "b",
                    3),
        "12a",
    };
    auto idx = BuildFmPatternReader(rows);
    EXPECT_FALSE(idx->PatternMatchIsExact(PatternOp::RegexMatch));
    const std::vector<std::string> patterns{
        "",
        "^$",
        ".*",
        "foo.*bar",
        "^foo.*bar$",
        "foo|bar",
        "foo|",
        "foo(?:d|l)",
        "(?:foo)?bar",
        "(?i)foo",
        "[a-z]+",
        "a{0,2}b",
        R"(\x41\141\.)",
        R"(a\|b)",
        R"(a\\b)",
        R"(\Qab.*\E)",
        R"(a\x00b)",
        R"(\d+a)",
        "é+b",
        "你好.*世界",
        "ABSENT.*",
    };
    for (const auto& pattern : patterns) {
        PartialRegexMatcher matcher(pattern);
        const auto candidates =
            idx->PatternMatch(pattern, PatternOp::RegexMatch);
        for (size_t i = 0; i < rows.size(); ++i) {
            const bool expected = matcher(rows[i]);
            EXPECT_EQ(candidates[i] && matcher(rows[i]), expected)
                << "pattern=" << pattern << " row=" << i;
        }
    }
    // Candidate membership alone must never be treated as a final result.
    const auto candidates =
        idx->PatternMatch("foo.*bar", PatternOp::RegexMatch);
    EXPECT_TRUE(
        candidates[4]);  // "barfoo" has both literals in the wrong order.
    EXPECT_FALSE(PartialRegexMatcher("foo.*bar")(rows[4]));
    EXPECT_LT(candidates.count(), rows.size());
}

TEST(FmIndexReaderTest, RegexGuardDeclinesEmptyAndAcceptsZeroHit) {
    auto reader = BuildFmReader({"apple", "banana"});
    EXPECT_FALSE(reader->Caps().predicate);
    EXPECT_FALSE(reader->Caps().value_lookup);
    const auto* patterns =
        dynamic_cast<const IPatternMatchReader*>(reader.get());
    ASSERT_NE(patterns, nullptr);
    for (const auto op : {PatternOp::PrefixMatch,
                          PatternOp::PostfixMatch,
                          PatternOp::InnerMatch}) {
        EXPECT_TRUE(patterns->ShouldUseForOp(op, ""));
    }
    EXPECT_TRUE(patterns->ShouldUseForOp(PatternOp::Match, "%ZZZ%"));
    EXPECT_FALSE(patterns->ShouldUseForOp(PatternOp::Match, ""));
    EXPECT_FALSE(patterns->ShouldUseForOp(PatternOp::RegexMatch, ""));
    EXPECT_TRUE(patterns->ShouldUseForOp(PatternOp::RegexMatch, "ZZZ.*"));
    LikePatternMatcher matcher("a%e");
    const auto candidates = patterns->PatternMatch("a%e", PatternOp::Match);
    EXPECT_TRUE(candidates[0] && matcher(std::string("apple")));
    EXPECT_FALSE(candidates[1] && matcher(std::string("banana")));
}

TEST(FmIndexReaderTest, RegexGuardAndFallback) {
    std::vector<std::string> rows(1000, std::string(500, 'x'));
    rows[500] += "RARE123";
    auto idx = BuildFmPatternReader(rows);
    EXPECT_TRUE(idx->ShouldUseForOp(PatternOp::RegexMatch, R"(RARE\d+)"));
    EXPECT_TRUE(idx->ShouldUseForOp(PatternOp::RegexMatch, "ABSENT.*"));
    EXPECT_TRUE(idx->ShouldUseForOp(PatternOp::RegexMatch, ".*RARE"));
    EXPECT_TRUE(idx->ShouldUseForOp(PatternOp::RegexMatch, "x.*RARE123"));
    EXPECT_EQ(idx->PatternMatch("x.*RARE123", PatternOp::RegexMatch).count(),
              1);
    for (const std::string pattern : {"x+", ".*", "", "RARE|", "(?i)rare"}) {
        EXPECT_FALSE(idx->ShouldUseForOp(PatternOp::RegexMatch, pattern))
            << pattern;
    }
    EXPECT_FALSE(
        idx->ShouldUseForOp(PatternOp::RegexMatch, std::string(4097, 'a')));
}

TEST(FmIndexReaderTest, RegexCandidatesKeepRarePrefixWhenInteriorIsCommon) {
    std::vector<std::string> rows(1000, std::string(500, 'x') + "COMMON");
    rows[500] = "RARE" + rows[500];
    auto idx = BuildFmPatternReader(rows);
    EXPECT_TRUE(idx->ShouldUseForOp(PatternOp::RegexMatch, "RARE.*COMMON"));
    const auto candidates =
        idx->PatternMatch("RARE.*COMMON", PatternOp::RegexMatch);
    EXPECT_EQ(candidates.count(), 1);
    EXPECT_TRUE(candidates[500]);
}

TEST(FmIndexReaderTest, RegexPreparationDoesNotCacheIndexStatistics) {
    std::vector<std::string> first_rows(1000, std::string(500, 'x') + "bar");
    first_rows[7] = "foo" + first_rows[7];
    std::vector<std::string> second_rows(1000, "foo" + std::string(500, 'x'));
    second_rows[9] += "bar";
    auto first = BuildFmPatternReader(first_rows);
    auto second = BuildFmPatternReader(second_rows);
    for (int repeat = 0; repeat < 2; ++repeat) {
        EXPECT_TRUE(first->ShouldUseForOp(PatternOp::RegexMatch, "foo.*bar"));
        EXPECT_TRUE(second->ShouldUseForOp(PatternOp::RegexMatch, "foo.*bar"));
        // The same cached requirements choose different rarest fragments.
        const auto first_candidates =
            first->PatternMatch("foo.*bar", PatternOp::RegexMatch);
        const auto second_candidates =
            second->PatternMatch("foo.*bar", PatternOp::RegexMatch);
        EXPECT_EQ(first_candidates.count(), 1);
        EXPECT_TRUE(first_candidates[7]);
        EXPECT_EQ(second_candidates.count(), 1);
        EXPECT_TRUE(second_candidates[9]);
    }
}

TEST(FmIndexReaderTest, RegexBooleanConditionsUseCurrentCounts) {
    std::vector<std::string> rows(
        1000, std::string(500, 'x') + "COMMON_LONG foo qux ERROR");
    rows[9] += "RARE bar timeout";
    rows[19] += "baz";
    auto index = BuildFmPatternReader(rows);
    for (const auto& [pattern, expected] :
         std::vector<std::pair<std::string, size_t>>{
             {".*COMMON_LONG.*RARE", 1},
             {"(ERROR|WARN).*timeout", 1},
             {"foo.*bar|baz.*qux", 2},
             {"foo.*ABSENT|baz.*qux", 1}}) {
        EXPECT_TRUE(index->ShouldUseForOp(PatternOp::RegexMatch, pattern));
        auto candidates = index->PatternMatch(pattern, PatternOp::RegexMatch);
        EXPECT_EQ(candidates.count(), expected) << pattern;
        PartialRegexMatcher matcher(pattern);
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(candidates[i] && matcher(rows[i]), matcher(rows[i]))
                << pattern;
        }
    }
    for (const std::string pattern :
         {"a|timeout", "(?i:foo)|timeout", "(foo)?|timeout"}) {
        EXPECT_FALSE(index->ShouldUseForOp(PatternOp::RegexMatch, pattern));
        EXPECT_EQ(index->PatternMatch(pattern, PatternOp::RegexMatch).count(),
                  rows.size());
    }
}

}  // namespace
}  // namespace milvus::index::test
