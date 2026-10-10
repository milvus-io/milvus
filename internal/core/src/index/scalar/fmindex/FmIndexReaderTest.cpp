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
                  PatternQueryPolicy::Unsupported);
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

}  // namespace
}  // namespace milvus::index::test
