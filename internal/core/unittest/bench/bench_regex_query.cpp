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

#include <benchmark/benchmark.h>
#include <string>
#include <vector>
#include <random>

#include "common/RegexQuery.h"

using namespace milvus;

// ============== Test Data Generation ==============

static std::vector<std::string>
GenerateRandomStrings(size_t count, size_t min_len, size_t max_len) {
    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 gen(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<> len_dist(min_len, max_len);
    std::uniform_int_distribution<> char_dist('a', 'z');

    for (size_t i = 0; i < count; ++i) {
        size_t len = len_dist(gen);
        std::string s;
        s.reserve(len);
        for (size_t j = 0; j < len; ++j) {
            s += static_cast<char>(char_dist(gen));
        }
        result.push_back(std::move(s));
    }
    return result;
}

static std::vector<std::string>
GenerateMatchingStrings(const std::string& pattern, size_t count) {
    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 gen(42);
    std::uniform_int_distribution<> char_dist('a', 'z');
    std::uniform_int_distribution<> len_dist(0, 10);

    for (size_t i = 0; i < count; ++i) {
        std::string s;
        for (char c : pattern) {
            if (c == '%') {
                // Add 0-10 random chars
                size_t extra = len_dist(gen);
                for (size_t j = 0; j < extra; ++j) {
                    s += static_cast<char>(char_dist(gen));
                }
            } else if (c == '_') {
                s += static_cast<char>(char_dist(gen));
            } else if (c == '\\') {
                // Skip escape char, next char is literal
                continue;
            } else {
                s += c;
            }
        }
        result.push_back(std::move(s));
    }
    return result;
}

// ============== Prefix Pattern: "abc%" ==============

static void
BM_BoostRegex_PrefixPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "abc%";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 5, 50);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_PrefixPattern);

static void
BM_RE2_PrefixPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "abc%";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 5, 50);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_PrefixPattern);

static void
BM_LikePatternMatcher_PrefixPattern(benchmark::State& state) {
    std::string pattern = "abc%";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 5, 50);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_PrefixPattern);

static void
BM_SmartPatternMatcher_PrefixPattern(benchmark::State& state) {
    std::string pattern = "abc%";
    SmartPatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 5, 50);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SmartPatternMatcher_PrefixPattern);

// ============== Suffix Pattern: "%abc" ==============

static void
BM_BoostRegex_SuffixPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%abc";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 5, 50);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_SuffixPattern);

static void
BM_RE2_SuffixPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%abc";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 5, 50);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_SuffixPattern);

static void
BM_LikePatternMatcher_SuffixPattern(benchmark::State& state) {
    std::string pattern = "%abc";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 5, 50);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_SuffixPattern);

// ============== Contains Pattern: "%abc%" ==============

static void
BM_BoostRegex_ContainsPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%abc%";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_ContainsPattern);

static void
BM_RE2_ContainsPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%abc%";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_ContainsPattern);

static void
BM_LikePatternMatcher_ContainsPattern(benchmark::State& state) {
    std::string pattern = "%abc%";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_ContainsPattern);

// ============== Complex Pattern: "a%b%c%d" ==============

static void
BM_BoostRegex_ComplexPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a%b%c%d";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_ComplexPattern);

static void
BM_RE2_ComplexPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a%b%c%d";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_ComplexPattern);

static void
BM_LikePatternMatcher_ComplexPattern(benchmark::State& state) {
    std::string pattern = "a%b%c%d";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_ComplexPattern);

// ============== Underscore Pattern: "a_c" ==============

static void
BM_BoostRegex_UnderscorePattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a_c";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 3, 10);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_UnderscorePattern);

static void
BM_RE2_UnderscorePattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a_c";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 3, 10);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_UnderscorePattern);

static void
BM_LikePatternMatcher_UnderscorePattern(benchmark::State& state) {
    std::string pattern = "a_c";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 3, 10);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_UnderscorePattern);

// ============== Mixed Pattern: "hello%_world%" ==============

static void
BM_BoostRegex_MixedPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "hello%_world%";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 15, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_MixedPattern);

static void
BM_RE2_MixedPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "hello%_world%";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 15, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_MixedPattern);

static void
BM_LikePatternMatcher_MixedPattern(benchmark::State& state) {
    std::string pattern = "hello%_world%";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 15, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_MixedPattern);

// ============== Long String Pattern ==============

static void
BM_BoostRegex_LongString(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%needle%";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(100, 1000, 5000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_LongString);

static void
BM_RE2_LongString(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%needle%";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(100, 1000, 5000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_LongString);

static void
BM_LikePatternMatcher_LongString(benchmark::State& state) {
    std::string pattern = "%needle%";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(100, 1000, 5000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_LongString);

// ============== Matching Strings (Best Case) ==============

static void
BM_BoostRegex_MatchingStrings(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "prefix%middle%suffix";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateMatchingStrings(pattern, 1000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_MatchingStrings);

static void
BM_RE2_MatchingStrings(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "prefix%middle%suffix";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateMatchingStrings(pattern, 1000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_MatchingStrings);

static void
BM_LikePatternMatcher_MatchingStrings(benchmark::State& state) {
    std::string pattern = "prefix%middle%suffix";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateMatchingStrings(pattern, 1000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_MatchingStrings);

// ============== Overlapping Pattern: "%aa%aa%" ==============

static std::vector<std::string>
GenerateOverlappingTestStrings() {
    std::vector<std::string> test_strings = {
        "aaa", "aaaa", "aaaaa", "aaaaaa", "xaayaaz", "aabaac", "abcaadefaag"};
    // Pad with random strings
    auto random_strings = GenerateRandomStrings(993, 5, 50);
    test_strings.insert(
        test_strings.end(), random_strings.begin(), random_strings.end());
    return test_strings;
}

static void
BM_BoostRegex_OverlappingPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%aa%aa%";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateOverlappingTestStrings();
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_OverlappingPattern);

static void
BM_RE2_OverlappingPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%aa%aa%";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateOverlappingTestStrings();
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_OverlappingPattern);

static void
BM_LikePatternMatcher_OverlappingPattern(benchmark::State& state) {
    std::string pattern = "%aa%aa%";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateOverlappingTestStrings();
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_OverlappingPattern);

// ============== Many Wildcards Pattern: "a%b%c%d%e%f%g%h" ==============

static void
BM_BoostRegex_ManyWildcards(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a%b%c%d%e%f%g%h";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 20, 200);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_ManyWildcards);

static void
BM_RE2_ManyWildcards(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a%b%c%d%e%f%g%h";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 20, 200);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_ManyWildcards);

static void
BM_LikePatternMatcher_ManyWildcards(benchmark::State& state) {
    std::string pattern = "a%b%c%d%e%f%g%h";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 20, 200);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_ManyWildcards);

// ============== Multiple Underscores: "a___b___c" ==============

static void
BM_BoostRegex_MultipleUnderscores(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a___b___c";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 9, 20);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_MultipleUnderscores);

static void
BM_RE2_MultipleUnderscores(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a___b___c";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 9, 20);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_MultipleUnderscores);

static void
BM_LikePatternMatcher_MultipleUnderscores(benchmark::State& state) {
    std::string pattern = "a___b___c";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 9, 20);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_MultipleUnderscores);

// ============== Exact Match (No Wildcards): "exactmatchstring" ==============

static void
BM_BoostRegex_ExactMatch(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "exactmatchstring";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 30);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_ExactMatch);

static void
BM_RE2_ExactMatch(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "exactmatchstring";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 30);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_ExactMatch);

static void
BM_LikePatternMatcher_ExactMatch(benchmark::State& state) {
    std::string pattern = "exactmatchstring";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 30);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_ExactMatch);

// ============== Match All: "%" ==============

static void
BM_BoostRegex_MatchAll(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_MatchAll);

static void
BM_RE2_MatchAll(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_MatchAll);

static void
BM_LikePatternMatcher_MatchAll(benchmark::State& state) {
    std::string pattern = "%";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_MatchAll);

// ============== Pathological Backtracking: "%a%a%a%a%b" ==============

static void
BM_BoostRegex_PathologicalBacktrack(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%a%a%a%a%b";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    // Generate strings with many 'a's but no 'b' to trigger worst-case backtracking
    std::vector<std::string> test_strings;
    test_strings.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
        test_strings.push_back(std::string(50 + (i % 50), 'a'));
    }
    size_t idx = 0;

    for (auto _ : state) {
        try {
            benchmark::DoNotOptimize(
                matcher(test_strings[idx % test_strings.size()]));
        } catch (const boost::regex_error&) {
            // Boost regex throws on pathological patterns - this is expected
            benchmark::DoNotOptimize(false);
        }
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_PathologicalBacktrack);

static void
BM_RE2_PathologicalBacktrack(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%a%a%a%a%b";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    std::vector<std::string> test_strings;
    test_strings.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
        test_strings.push_back(std::string(50 + (i % 50), 'a'));
    }
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_PathologicalBacktrack);

static void
BM_LikePatternMatcher_PathologicalBacktrack(benchmark::State& state) {
    std::string pattern = "%a%a%a%a%b";
    LikePatternMatcher matcher(pattern);

    std::vector<std::string> test_strings;
    test_strings.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
        test_strings.push_back(std::string(50 + (i % 50), 'a'));
    }
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_PathologicalBacktrack);

// ============== Very Long Pattern ==============

static void
BM_BoostRegex_VeryLongPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "start%middle1%middle2%middle3%middle4%middle5%end";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateMatchingStrings(pattern, 1000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_VeryLongPattern);

static void
BM_RE2_VeryLongPattern(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "start%middle1%middle2%middle3%middle4%middle5%end";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateMatchingStrings(pattern, 1000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_VeryLongPattern);

static void
BM_LikePatternMatcher_VeryLongPattern(benchmark::State& state) {
    std::string pattern = "start%middle1%middle2%middle3%middle4%middle5%end";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateMatchingStrings(pattern, 1000);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_VeryLongPattern);

// ============== Mixed Wildcards and Underscores: "%a_b%c_d%" ==============

static void
BM_BoostRegex_MixedWildcardsUnderscores(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%a_b%c_d%";
    auto regex_pattern = translator(pattern);
    BoostRegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_MixedWildcardsUnderscores);

static void
BM_RE2_MixedWildcardsUnderscores(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "%a_b%c_d%";
    auto regex_pattern = translator(pattern);
    RegexMatcher matcher(regex_pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_MixedWildcardsUnderscores);

static void
BM_LikePatternMatcher_MixedWildcardsUnderscores(benchmark::State& state) {
    std::string pattern = "%a_b%c_d%";
    LikePatternMatcher matcher(pattern);

    auto test_strings = GenerateRandomStrings(1000, 10, 100);
    size_t idx = 0;

    for (auto _ : state) {
        benchmark::DoNotOptimize(
            matcher(test_strings[idx % test_strings.size()]));
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_MixedWildcardsUnderscores);

// ============== Pattern Compilation Benchmark ==============

static void
BM_BoostRegex_Compilation(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a%b%c%d%e";
    auto regex_pattern = translator(pattern);

    for (auto _ : state) {
        BoostRegexMatcher matcher(regex_pattern);
        benchmark::DoNotOptimize(matcher);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_BoostRegex_Compilation);

static void
BM_RE2_Compilation(benchmark::State& state) {
    PatternMatchTranslator translator;
    std::string pattern = "a%b%c%d%e";
    auto regex_pattern = translator(pattern);

    for (auto _ : state) {
        RegexMatcher matcher(regex_pattern);
        benchmark::DoNotOptimize(matcher);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RE2_Compilation);

static void
BM_LikePatternMatcher_Compilation(benchmark::State& state) {
    std::string pattern = "a%b%c%d%e";

    for (auto _ : state) {
        LikePatternMatcher matcher(pattern);
        benchmark::DoNotOptimize(matcher);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_LikePatternMatcher_Compilation);

BENCHMARK_MAIN();
