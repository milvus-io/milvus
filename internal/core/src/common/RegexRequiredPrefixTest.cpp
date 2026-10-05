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

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "common/RegexQuery.h"
#include "common/Volnitsky.h"
#include "index/fmindex/FMIndex.h"

using milvus::PartialRegexMatcher;

TEST(RegexRequiredPrefix, ExtractionAndBounds) {
    EXPECT_EQ(PartialRegexMatcher("foo.*bar").RequiredPrefix(), "foo");
    EXPECT_EQ(PartialRegexMatcher("foo|foobar").RequiredPrefix(), "foo");
    EXPECT_EQ(PartialRegexMatcher(R"(\x41\141\.)").RequiredPrefix(), "Aa.");
    EXPECT_EQ(PartialRegexMatcher(R"(\Qfoo.*\E)").RequiredPrefix(), "foo.*");
    for (const std::string pattern :
         {"", ".*", "a*", "foo|", "foo|bar", "(?i)foo", "[a-z]+"}) {
        EXPECT_TRUE(PartialRegexMatcher(pattern).RequiredPrefix().empty())
            << pattern;
    }
    auto prefix = PartialRegexMatcher(std::string(100, 'a')).RequiredPrefix();
    EXPECT_FALSE(prefix.empty());
    EXPECT_LE(prefix.size(), 64);
    EXPECT_TRUE(
        PartialRegexMatcher(std::string(4097, 'a')).RequiredPrefix().empty());
}

TEST(RegexRequiredPrefix, DeclinesExternalWordBoundaryContext) {
    for (const std::string pattern : {R"(\Bfoo|bar)",
                                      R"(\b-foo|bar)",
                                      R"((?:\Bfoo|bar))",
                                      R"(a?\Bfoo|bar)",
                                      R"(foo\B|bar)",
                                      R"(\bfoo)",
                                      R"(\\Bfoo)",
                                      R"(\Q\bfoo\E)"}) {
        EXPECT_TRUE(PartialRegexMatcher(pattern).RequiredPrefix().empty())
            << pattern;
    }
    EXPECT_TRUE(PartialRegexMatcher(R"(\Bfoo|bar)")(std::string("afoo")));
    EXPECT_TRUE(PartialRegexMatcher(R"(\b-foo|bar)")(std::string("a-foo")));
}

TEST(RegexRequiredPrefix, IndexRequirementsKeepIndependentSelectiveLiterals) {
    for (const auto& [pattern, expected] :
         std::vector<std::pair<std::string, std::vector<std::string>>>{
             {".*RARE", {"RARE"}},
             {"x.*RARE123END", {"RARE123END", "x"}},
             {"RARE.*COMMON", {"COMMON", "RARE"}},
             {"foo.*foobar", {"foobar"}},
             {"[a]needle", {"aneedle"}},
             {"foo|foobar", {"foo"}},
             {R"(.*\x41)", {"A"}},
             {R"(\Bfoo)", {"foo"}},
             {R"(\Bfoo|bar)", {}},
             {"foo|bar", {}},
             {"foo|", {}},
             {"(?i)foo", {}},
             {"a*", {}},
             {"", {}}}) {
        EXPECT_EQ(PartialRegexMatcher(pattern).RequiredIndexLiterals(), expected)
            << pattern;
    }
    EXPECT_EQ(PartialRegexMatcher(".*" + std::string(500, 'x') + "END")
                  .RequiredIndexLiterals(),
              (std::vector<std::string>{std::string(64, 'x'),
                                       std::string(61, 'x') + "END"}));
    EXPECT_EQ(PartialRegexMatcher("RARE.*BEGIN" + std::string(100, 'x') + "END")
                  .RequiredIndexLiterals(),
              (std::vector<std::string>{"BEGIN" + std::string(59, 'x'),
                                       std::string(61, 'x') + "END",
                                       "RARE"}));
    EXPECT_TRUE(PartialRegexMatcher(std::string(4097, 'a'))
                    .RequiredIndexLiterals()
                    .empty());
}

TEST(RegexRequiredPrefix, RawScanPreservesSelectiveInteriorLiterals) {
    // Lock down useful pruning independently of noisy wall-clock thresholds.
    std::vector<std::string> rows(20000, std::string(500, 'x') + "COMMON");
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i % 1000 == 0) {
            rows[i] += "RARE123END";
        } else if (i % 1000 == 1) {
            rows[i] += "RAREwrong";
        }
    }
    for (const auto& [pattern, expected_literal, expected_checks] :
         std::vector<std::tuple<std::string, std::string, size_t>>{
             {".*RARE", "RARE", 40},
             {"x.*RARE123END", "RARE123END", 20},
             {".*RARE[0-9]+END", "RARE", 40},
             {"x.*(RARE123END)", "RARE123END", 20},
             {R"(.*RARE\d+END)", "RARE", 40},
             {"x.*RARE123END+", "RARE123END", 20},
             {"x.*RARE123END?", "RARE123EN", 20},
             {"x.*RARE123END{1}", "RARE123END", 20},
             {"x.*(RARE123END){1,2}", "RARE123END", 20},
             {R"(.*RARE\w+END)", "RARE", 40},
             {R"(.*RARE\p{L}+END)", "RARE", 40},
             {"(?s).*RARE123END", "RARE123END", 20},
             {"x.*(?P<n>RARE123END)", "RARE123END", 20},
             {"x.*RARE(123)END", "RARE123END", 20},
             {R"(.*\QRARE123END\E)", "RARE123END", 20},
             {"x.*R{1}ARE123END", "RARE123END", 20},
             {".*x{500}COMMONRARE123END",
              std::string(500, 'x') + "COMMONRARE123END",
              20},
             {"x.*RARE123ENDx{100}",
              "RARE123END" + std::string(100, 'x'),
              0}}) {
        PartialRegexMatcher matcher(pattern);
        const auto literal = matcher.RequiredLiteral();
        EXPECT_EQ(literal, expected_literal);
        milvus::VolnitskySearcher searcher(literal);
        size_t checks = 0;
        for (const auto& row : rows) {
            const bool candidate = literal.empty() || searcher.contains(row);
            checks += candidate;
            EXPECT_EQ(candidate && matcher(row), matcher(row));
        }
        EXPECT_EQ(checks, expected_checks) << pattern;
    }
}

TEST(RegexRequiredPrefix, RawScanSelectivityAcrossSyntaxFamilies) {
    std::vector<std::string> rows(20000, std::string(500, 'x') + "COMMON");
    for (size_t i = 0; i < rows.size(); i += 1000) {
        rows[i] += "RARE123END";
    }
    // Equivalent required strings expressed through independent syntax forms.
    // Positive lower bounds must retain selectivity; zero lower bounds must
    // never use the optional body's literal as a correctness gate.
    for (const std::string spelling : {"RARE123END",
                                       "RARE(123)END",
                                       "RARE(?:123)END",
                                       "(?P<n>RARE123END)",
                                       R"(\QRARE123END\E)",
                                       R"(RARE\x31\062\x{33}END)",
                                       "R{1}ARE123END",
                                       "(?s:RARE123END)"}) {
        for (const auto& [quantifier, expected_checks] :
             std::vector<std::pair<std::string, size_t>>{{"", 20},
                                                         {"+", 20},
                                                         {"+?", 20},
                                                         {"{1}", 20},
                                                         {"{1,}", 20},
                                                         {"{1,2}?", 20},
                                                         {"{2}", 20},
                                                         {"?", 20000},
                                                         {"*", 20000},
                                                         {"{0}", 20000},
                                                         {"{0,2}", 20000}}) {
            const auto pattern = "x.*(?:" + spelling + ")" + quantifier;
            PartialRegexMatcher matcher(pattern);
            const auto literal = matcher.RequiredLiteral();
            milvus::VolnitskySearcher searcher(literal);
            size_t checks = 0;
            for (const auto& row : rows) {
                const bool candidate = searcher.contains(row);
                checks += candidate;
                // Only two distinct row values, so check semantics below once
                // per value rather than performing millions of RE2 calls.
            }
            EXPECT_LE(checks, expected_checks) << pattern;
            for (size_t i : {0, 1}) {
                EXPECT_EQ(searcher.contains(rows[i]) && matcher(rows[i]),
                          matcher(rows[i]))
                    << pattern;
            }
        }
    }
}

TEST(RegexRequiredPrefix, LargeRepeatedLiteralsRemainSound) {
    for (const auto& [pattern, row] :
         std::vector<std::pair<std::string, std::string>>{
             {".*(a{70})b", std::string(70, 'a') + "b"},
             {".*a{70}bc{70}",
              std::string(70, 'a') + "b" + std::string(70, 'c')},
             {".*(abcdefghij){20}Z",
              [] {
                  std::string s;
                  for (int i = 0; i < 20; ++i) s += "abcdefghij";
                  return s + "Z";
              }()},
             {".*(é{40})END", [] {
                  std::string s;
                  for (int i = 0; i < 40; ++i) s += "é";
                  return s + "END";
              }()}}) {
        PartialRegexMatcher matcher(pattern);
        ASSERT_TRUE(matcher(row));
        const auto literal = matcher.RequiredLiteral();
        EXPECT_LE(literal.size(), 4096);
        EXPECT_NE(row.find(literal), std::string::npos) << pattern;
        for (const auto& requirement : matcher.RequiredIndexLiterals()) {
            EXPECT_LE(requirement.size(), 64);
            EXPECT_NE(row.find(requirement), std::string::npos) << pattern;
        }
    }
}

TEST(RegexRequiredPrefix, RawLiteralGrammarAndFallback) {
    for (const auto& [pattern, literal] :
         std::vector<std::pair<std::string, std::string>>{
             {".*RARE", "RARE"},
             {"^x.*?RARE123END$", "RARE123END"},
             {"x.+RARE", "RARE"},
             {"x.??RARE", "RARE"},
             {R"(.*foo\.bar)", "foo.bar"},
             {R"(.*foo\|bar)", "foo|bar"},
             {R"(.*foo\\bar)", R"(foo\bar)"},
             {"x.*你好世界", "你好世界"},
             {".*RARE[0-9]+END", "RARE"},
             {"x.*(RARE123END)", "RARE123END"},
             {"x.*(?:RARE123END)+?", "RARE123END"},
             {".*((RARE123END))", "RARE123END"},
             {".*(RARE123END)?OK", "OK"},
             {".*((RARE123END)+)?OK", "OK"},
             {".*(RARE123END)*?OK", "OK"},
             {".*SAFE(OPTIONAL_LONGER)?", "SAFE"},
             {".*((OPTIONAL)?RARE)", "RARE"},
             {".*(RARE)?", ""},
             {".*RARE[]a]+END", "RARE"},
             {".*RARE[^]a]+END", "RARE"},
             {".*RARE[[:digit:]]+END", "RARE"},
             {R"(.*RARE[a\]]+END)", "RARE"},
             {R"(.*RARE[\d]+END)", "RARE"},
             {R"(.*RARE[()|]+END)", "RARE"},
             {".*", ""}}) {
        EXPECT_EQ(PartialRegexMatcher(pattern).RequiredLiteral(), literal)
            << pattern;
    }
    for (const auto& [pattern, literal] :
         std::vector<std::pair<std::string, std::string>>{
             {"x.*RARE?", "RAR"},
             {"x.*RARE*", "RAR"},
             {"x.*RARE+", "RARE"},
             {"x.*RARE{0}", "RAR"},
             {"x.*(?P<name>RARE)", "RARE"},
             {"x.*RARE(?i)end", "RARE"},
             {R"(.*RARE[\x41]END)", "RARE"},
             {R"(.*\x41)", "A"},
             {R"(.*\141)", "a"},
             {R"(.*\Qfoo.*\E)", "foo.*"},
             {".*RARE(123)END", "RARE123END"},
             {".*a{0}RARE", "RARE"},
             {".*(a{0}RARE){0}", ""},
             {".*(a{0}RARE){2}", "RARERARE"},
             {".*RAREé?", "RARE"},
             {".*RAREé+", "RAREé"},
             {R"(.*\Qab\E+)", "ab"},
             {R"(.*a\Q\E?b)", "b"},
             {R"(.*\0\Q\E12)",
              std::string("\0"
                          "12",
                          3)},
             {R"(.*\Qabc)", "abc"},
             {"(?i).*FOO(?-i:RARE)bar", "RARE"},
             {".*RARE(?i:foo)END", "RARE"},
             {R"(.*RARE\B)", "RARE"}}) {
        EXPECT_EQ(PartialRegexMatcher(pattern).RequiredLiteral(), literal)
            << pattern;
    }
    // Unsupported alternatives discard every tentative requirement, including
    // branches with external context. Noncanonical braces are not repetitions.
    for (const std::string pattern : {"x.*RARE|bar",
                                      "x.*(RARE|bar)",
                                      R"(x.*RARE\B|bar)",
                                      R"(\b-foo|bar)",
                                      "x.*RARE{01}",
                                      "x.*RARE{literal}",
                                      R"(.*a{\Q2\E})"}) {
        PartialRegexMatcher matcher(pattern);
        EXPECT_EQ(matcher.RequiredLiteral(), matcher.RequiredPrefix())
            << pattern;
    }
    EXPECT_EQ(
        PartialRegexMatcher(".*" + std::string(100, 'a')).RequiredLiteral(),
        std::string(100, 'a'));
    EXPECT_TRUE(
        PartialRegexMatcher(std::string(4097, 'a')).RequiredLiteral().empty());
    EXPECT_EQ(PartialRegexMatcher(".*" + std::string(64, '(') + "RARE" +
                                  std::string(64, ')'))
                  .RequiredLiteral(),
              "RARE");
    EXPECT_TRUE(PartialRegexMatcher(".*" + std::string(65, '(') + "RARE" +
                                    std::string(65, ')'))
                    .RequiredLiteral()
                    .empty());
}

TEST(RegexRequiredPrefix, EveryMatchContainsTheRequirement) {
    std::vector<std::string> patterns{
        "",
        "^$",
        ".*",
        ".+",
        "a*",
        "a+",
        "a?",
        "a{0,3}",
        "a{2,3}",
        "a.*b",
        "^a.*b$",
        "a|b",
        "a|",
        "ab|ac",
        "(?:ab|ac)d",
        "(ab)?c",
        "(a.*b)+c",
        "a(bc){0}d",
        "(?:a?)*b",
        "[ab]+c",
        "[^a]*b",
        "[[:alpha:]]+",
        "[]a]b",
        "(?i)ab",
        "a(?i:bc)d",
        "(?i:a)(?-i:bc)",
        "(?m)^a$",
        "(?-s)a.b",
        "(?P<name>a)b",
        R"(\x61b)",
        R"(\x{61}b)",
        R"(\141b)",
        R"(\Qab.*\E)",
        R"(\pL+a)",
        R"(\p{Han}+a)",
        R"(\d{2}a)",
        R"(a\nb)",
        R"(a\tb)",
        R"(a\x00b)",
        R"(a\|b)",
        R"(a\\b)",
        R"(\ba)",
        R"(\Ba)",
        R"(a\b)",
        R"(a\B)",
        R"(\Bfoo|bar)",
        R"(\b-foo|bar)",
        R"((?:\Bfoo|bar))",
        R"(a?\Bfoo|bar)",
        R"(foo\B|bar)",
        R"(a\C+b)",
        "é?b",
        "é+b",
        "你好.*世界",
        "(?:你好|您好)世界",
        "(?i)k",
        ".*ab",
        "a.*?bc",
        "a.+?bc",
        "a.??bc",
        "a.?bc",
        "a.b.c",
        "^.*ab$",
        R"(.*a\|b)",
        R"(.*a\\b)",
        R"(.*\x61b)",
        R"(.*\141b)",
        R"(.*\pL)",
        R"(.*\Qab.*\E)",
        R"(.*a\x00b)",
        "a.*bc?",
        "a.*bc*",
        "a.*bc+",
        "a.*bc{0}",
        "a.*(bc)?",
        "a.*bc|b",
        "a.*[bc]",
        "a.*bc(?i)d",
        R"(a.*bc\B|b)",
        R"(.*a\|b?)",
        "a.*é?b",
        R"(.*\0\Q\E12)",
        R"(.*a{\Q2\E})",
        R"(.*a\Q\E?b)",
        R"(.*\Qab\E+)",
        R"(.*\Qabc)",
    };
    // Cross classes and nested groups with mandatory/optional/lazy quantifiers
    // and following literals. The leading wildcard forces interior extraction
    // to stand on its own, rather than hiding bugs behind an RE2 prefix.
    for (const std::string prefix : {"", ".*"}) {
        for (const std::string atom :
             {"a",          "ab",          "é",           "[ab]",
              "[]a]",       "[^]a]",       "[[:alpha:]]", R"([a\]])",
              R"([\d])",    "(ab)",        "(?:ab)",      "((ab)?c)",
              "(a(bc)*)",   R"(\d)",       R"(\w)",       R"(\pL)",
              R"(\p{Han})", R"(\x61)",     R"(\x{e9})",   R"(\141)",
              R"(\Qab\E)",  R"(\Qab.*\E)", R"(\n)",       R"(\x00)",
              "(?i:ab)",    "(?-i:ab)",    "(?s:ab)",     "(?P<name>ab)",
              "(a{0}bc)",   "(ab{2,3}c)",  "(a(b)c)"}) {
            for (const std::string quantifier : {"",
                                                 "?",
                                                 "*",
                                                 "+",
                                                 "??",
                                                 "*?",
                                                 "+?",
                                                 "{0}",
                                                 "{1,2}",
                                                 "{1}",
                                                 "{2}",
                                                 "{0,2}",
                                                 "{2,3}?",
                                                 "{2,}"}) {
                for (const std::string suffix : {"",
                                                 "b",
                                                 "(abc)?",
                                                 "[ab]+c",
                                                 "(?i)ABC",
                                                 "(?-i:bc)",
                                                 "(abc){0}d"}) {
                    patterns.push_back(prefix + atom + quantifier + suffix);
                }
            }
        }
    }
    std::vector<std::string> rows{
        "",
        "Aa.",
        "ab.*",
        "abc",
        "abbc",
        "ac",
        "ad",
        "ab|c",
        "a|b",
        "a\\b",
        "a\nb",
        "a\tb",
        std::string("a\0b", 3),
        "12a",
        "zabz",
        "aab",
        "AB",
        "aBCd",
        "aaab",
        "ééb",
        "你好世界",
        "您好世界",
        "K",
        std::string("a\xff"
                    "b",
                    3),
        "foo.*",
        "afoo",
        "a-foo",
        "fooa",
        "bar",
        "]b",
        "]ab",
        "[ab",
        "a]b",
        std::string("\0"
                    "12",
                    3),
        "a{2}",
    };
    // Exhaust all short strings over an alphabet with literals, whitespace,
    // regex syntax, and NUL; no sampling can miss a short optional branch.
    const std::string alphabet("abc|\n\0", 6);
    std::vector<std::string> level{std::string()};
    for (int length = 1; length <= 4; ++length) {
        std::vector<std::string> next;
        for (const auto& prefix : level) {
            for (char c : alphabet) {
                next.push_back(prefix + c);
            }
        }
        rows.insert(rows.end(), next.begin(), next.end());
        level = std::move(next);
    }
    std::vector<std::string_view> docs(rows.begin(), rows.end());
    milvus::index::fmindex::FMIndex index;
    index.Build(docs, 8);
    RecordProperty("patterns", static_cast<int>(patterns.size()));
    RecordProperty("rows", static_cast<int>(rows.size()));
    for (const auto& pattern : patterns) {
        PartialRegexMatcher matcher(pattern);
        const auto prefix = matcher.RequiredPrefix();
        ASSERT_LE(prefix.size(), 64);
        const auto literal = matcher.RequiredLiteral();
        ASSERT_LE(literal.size(), 4096);
        const auto requirements = matcher.RequiredIndexLiterals();
        ASSERT_LE(requirements.size(), 3);
        std::vector<bool> index_candidates(rows.size(), true);
        for (const auto& requirement : requirements) {
            ASSERT_FALSE(requirement.empty());
            ASSERT_LE(requirement.size(), 64);
            std::vector<bool> hits(rows.size(), false);
            index.VisitMatchingDocs(
                reinterpret_cast<const uint8_t*>(requirement.data()),
                requirement.size(),
                [&](uint64_t row) { hits.at(row) = true; });
            for (size_t i = 0; i < rows.size(); ++i) {
                index_candidates[i] = index_candidates[i] && hits[i];
            }
        }
        milvus::VolnitskySearcher searcher(literal);
        std::vector<bool> candidates(rows.size(), prefix.empty());
        if (!prefix.empty()) {
            index.VisitMatchingDocs(
                reinterpret_cast<const uint8_t*>(prefix.data()),
                prefix.size(),
                [&](uint64_t row) { candidates.at(row) = true; });
        }
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& row = rows[i];
            if (matcher(row)) {
                EXPECT_NE(row.find(prefix), std::string::npos)
                    << "pattern=" << pattern << " row=" << row;
                EXPECT_TRUE(candidates[i])
                    << "pattern=" << pattern << " row=" << row;
                // Even intersecting every requirement must retain all matches;
                // choosing only the rarest one therefore remains sound.
                EXPECT_TRUE(index_candidates[i])
                    << "index requirements rejected pattern=" << pattern
                    << " row=" << row;
                EXPECT_NE(row.find(literal), std::string::npos)
                    << "pattern=" << pattern << " row=" << row;
                EXPECT_TRUE(searcher.contains(row))
                    << "scan prefilter rejected pattern=" << pattern
                    << " row=" << row;
            }
        }
    }
}

// Compare the pre-change needle for these fixtures, the prefix-only
// regression, and the production raw-scan requirement on identical rows.
// The fixed reference needles were checked against the legacy NGRAM extractor;
// they are NOT a general-purpose regex extractor or a correctness oracle.
TEST(RegexRequiredPrefix, DISABLED_RawScanBaselineBenchmark) {
    std::vector<std::string> rows(20000, std::string(500, 'x') + "COMMON");
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i % 1000 == 0) {
            rows[i] += "RARE123END";
        } else if (i % 1000 == 1) {
            rows[i] += "RAREwrong";
        }
    }
    for (const auto& [name, pattern, baseline_literal] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"leading_wildcard", ".*RARE", "RARE"},
             {"common_prefix", "x.*RARE123END", "RARE123END"},
             {"character_class", ".*RARE[0-9]+END", "RARE"},
             {"capture_group", "x.*(RARE123END)", "RARE123END"},
             {"shorthand", R"(.*RARE\d+END)", "RARE"},
             {"literal_plus", "x.*RARE123END+", "RARE123END"},
             {"literal_optional", "x.*RARE123END?", "RARE123EN"},
             {"counted_group", "x.*(RARE123END){1,2}", "RARE123END"},
             {"flags", "(?s).*RARE123END", "RARE123END"},
             {"named_group", "x.*(?P<n>RARE123END)", "RARE123END"},
             {"group_concat", "x.*RARE(123)END", "RARE123END"},
             {"quoted", R"(.*\QRARE123END\E)", "RARE123END"},
             {"long_literal",
              ".*x{500}COMMONRARE123END",
              std::string(500, 'x') + "COMMONRARE123END"},
             {"long_suffix",
              "x.*RARE123ENDx{100}",
              "RARE123END" + std::string(100, 'x')}}) {
        PartialRegexMatcher canonical(pattern);
        std::vector<bool> expected;
        for (const auto& row : rows) {
            expected.push_back(canonical(row));
        }
        std::vector<double> samples[3];
        const std::string modes[]{
            "fixture_baseline", "prefix_only", "repaired"};
        // Rotate execution order, warm up each mode, then take nine samples.
        for (int round = 0; round < 10; ++round) {
            for (int offset = 0; offset < 3; ++offset) {
                const int mode = (round + offset) % 3;
                const auto start = std::chrono::steady_clock::now();
                PartialRegexMatcher matcher(pattern);
                const auto literal = mode == 0   ? baseline_literal
                                     : mode == 1 ? matcher.RequiredPrefix()
                                                 : matcher.RequiredLiteral();
                milvus::VolnitskySearcher searcher(literal);
                std::vector<bool> result(rows.size(), false);
                size_t checks = 0;
                for (size_t i = 0; i < rows.size(); ++i) {
                    if (literal.empty() || searcher.contains(rows[i])) {
                        ++checks;
                        result[i] = matcher(rows[i]);
                    }
                }
                const auto us = std::chrono::duration<double, std::micro>(
                                    std::chrono::steady_clock::now() - start)
                                    .count();
                ASSERT_EQ(result, expected);
                if (round > 0) {
                    samples[mode].push_back(us);
                } else {
                    RecordProperty(name + "_" + modes[mode] + "_literal",
                                   literal);
                    RecordProperty(name + "_" + modes[mode] + "_checks",
                                   static_cast<int>(checks));
                }
            }
        }
        for (int mode = 0; mode < 3; ++mode) {
            std::sort(samples[mode].begin(), samples[mode].end());
            RecordProperty(name + "_" + modes[mode] + "_median_us",
                           std::to_string(samples[mode][4]));
        }
    }
}

// Isolate extraction cost from RE2 compilation and row scanning. In particular,
// long literal runs should not allocate a new summary for every input byte.
TEST(RegexRequiredPrefix, DISABLED_AnalysisBenchmark) {
    const std::vector<std::pair<std::string, std::string>> workloads{
        {"short", R"(RARE\d+END)"},
        {"empty_branch", "RARE|"},
        {"optional", "(?:RARE123END)?"},
        {"long_run", ".*" + std::string(3000, 'x') + "RARE123END"},
        {"long_repeat", ".*x{500}COMMONRARE123END"},
        {"quoted", R"(.*\QRARE123END\E)"},
        {"escapes", R"(.*RARE\x31\062\x{33}END)"},
        {"group_concat", "x.*RARE(123)END"},
    };
    constexpr int iterations = 200;
    RecordProperty("iterations_per_sample", iterations);
    for (const auto& [name, pattern] : workloads) {
        PartialRegexMatcher matcher(pattern);
        for (bool prefix : {false, true}) {
            const auto expected = prefix ? matcher.RequiredPrefix()
                                         : matcher.RequiredLiteral();
            std::vector<double> samples;
            for (int round = 0; round < 10; ++round) {
                size_t bytes = 0;
                const auto start = std::chrono::steady_clock::now();
                for (int i = 0; i < iterations; ++i) {
                    bytes += (prefix ? matcher.RequiredPrefix()
                                     : matcher.RequiredLiteral())
                                 .size();
                }
                const auto us = std::chrono::duration<double, std::micro>(
                                    std::chrono::steady_clock::now() - start)
                                    .count() /
                                iterations;
                ASSERT_EQ(bytes, iterations * expected.size());
                if (round > 0) {
                    samples.push_back(us);
                }
            }
            std::sort(samples.begin(), samples.end());
            RecordProperty(name + (prefix ? "_prefix_us" : "_literal_us"),
                           std::to_string(samples[samples.size() / 2]));
        }
    }
}

// Component baseline, independent of segment storage. The production executor
// benchmark (FMIndex.DISABLED_RegexEndToEndBenchmark) additionally measures
// expression dispatch and sealed-column reads. Neither asserts a speedup.
TEST(RegexRequiredPrefix, DISABLED_ComponentBenchmark) {
    std::vector<std::string> rows(20000, std::string(500, 'x'));
    for (size_t i = 0; i < rows.size(); ++i) {
        rows[i] += "COMMON";
        if (i % 1000 == 0) {
            rows[i] += "RARE123END";
        } else if (i % 1000 == 1) {
            rows[i] = "RAREwrong" + rows[i];
        }
    }
    std::vector<std::string_view> docs(rows.begin(), rows.end());
    milvus::index::fmindex::FMIndex index;
    index.Build(docs, 8);
    size_t bytes = 0;
    for (const auto& row : rows) {
        bytes += row.size();
    }
    const std::vector<std::pair<std::string, std::string>> workloads{
        {"selective", R"(RARE\d+END)"},
        {"unselective", "COMMON.*"},
        {"interior", ".*RARE"},
        {"common_prefix", "x.*RARE123END"},
        {"long_literal", ".*x{500}COMMONRARE123END"},
        {"rare_prefix", "RARE.*COMMON"},
        {"no_literal", ".*"},
        {"empty_branch", "RARE|"},
        {"zero_hits", "ABSENT.*"},
    };
    RecordProperty("rows", static_cast<int>(rows.size()));
    RecordProperty("bytes", std::to_string(bytes));
    RecordProperty("sa_sample_rate", 8);
    RecordProperty("fmindex_cost_ratio", "0.001");
    const std::string modes[]{"full_re2", "raw", "prefix_fm", "literal_fm"};
    for (const auto& [name, pattern] : workloads) {
        // Mirror separate wrapper calls: the guard and candidate generation
        // each compile/analyze/count, in addition to the canonical matcher.
        // A declined FM guard uses the production raw literal + Volnitsky
        // prefilter, so improvements are not inflated by a pure RE2 baseline.
        auto run = [&](int mode) {
            PartialRegexMatcher matcher(pattern);
            auto select = [&]() {
                PartialRegexMatcher analysis(pattern);
                auto parts = mode == 3 ? analysis.RequiredIndexLiterals()
                                       : std::vector<std::string>{};
                if (mode == 2) {
                    auto prefix = analysis.RequiredPrefix();
                    if (!prefix.empty()) {
                        parts.push_back(std::move(prefix));
                    }
                }
                std::string best;
                uint64_t minimum = 0;
                for (const auto& part : parts) {
                    const auto count = index.Count(
                        reinterpret_cast<const uint8_t*>(part.data()),
                        part.size());
                    if (best.empty() || count < minimum) {
                        best = part;
                        minimum = count;
                    }
                }
                return std::make_pair(std::move(best), minimum);
            };
            std::vector<bool> candidates(rows.size(), true);
            bool accepted = false;
            if (mode >= 2) {
                const auto [guard_literal, count] = select();
                accepted = !guard_literal.empty() &&
                           (count == 0 || count * 8.0 < bytes * 0.001);
                if (accepted) {
                    const auto [literal, unused_count] = select();
                    std::fill(candidates.begin(), candidates.end(), false);
                    index.VisitMatchingDocs(
                        reinterpret_cast<const uint8_t*>(literal.data()),
                        literal.size(),
                        [&](uint64_t row) { candidates.at(row) = true; });
                }
            }
            const auto candidate_count =
                std::count(candidates.begin(), candidates.end(), true);
            const auto raw_literal = mode != 0 && !accepted
                                         ? matcher.RequiredLiteral()
                                         : std::string{};
            std::unique_ptr<milvus::VolnitskySearcher> searcher;
            if (!raw_literal.empty()) {
                searcher =
                    std::make_unique<milvus::VolnitskySearcher>(raw_literal);
            }
            size_t checks = 0;
            for (size_t i = 0; i < rows.size(); ++i) {
                if (candidates[i] &&
                    (!searcher || searcher->contains(rows[i]))) {
                    ++checks;
                    candidates[i] = matcher(rows[i]);
                } else {
                    candidates[i] = false;
                }
            }
            return std::make_tuple(
                std::move(candidates), candidate_count, checks, accepted);
        };
        auto [expected, unused_count, unused_checks, unused_accepted] = run(0);
        std::vector<double> times[4];
        // One warmup and nine samples per mode, rotating their order.
        for (int round = 0; round < 10; ++round) {
            for (int offset = 0; offset < 4; ++offset) {
                const int mode = (round + offset) % 4;
                const auto start = std::chrono::steady_clock::now();
                auto [actual, candidates, checks, accepted] = run(mode);
                const auto elapsed =
                    std::chrono::duration<double, std::micro>(
                        std::chrono::steady_clock::now() - start)
                        .count();
                ASSERT_EQ(actual, expected) << name << " mode=" << modes[mode];
                if (round > 0) {
                    times[mode].push_back(elapsed);
                } else {
                    RecordProperty(name + "_" + modes[mode] + "_candidates",
                                   static_cast<int>(candidates));
                    RecordProperty(name + "_" + modes[mode] + "_checks",
                                   static_cast<int>(checks));
                    RecordProperty(name + "_" + modes[mode] + "_uses_index",
                                   accepted);
                }
            }
        }
        for (int mode = 0; mode < 4; ++mode) {
            std::sort(times[mode].begin(), times[mode].end());
            RecordProperty(name + "_" + modes[mode] + "_us",
                           std::to_string(times[mode][4]));
        }
    }
}
