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

#include <gtest/gtest.h>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/RegexQuery.h"

TEST(IsSpecial, Demo) {
    std::string special_bytes(R"(\.+*?()|[]{}^$)");
    std::unordered_set<char> specials;
    for (char b : special_bytes) {
        specials.insert(b);
    }
    for (char c = std::numeric_limits<int8_t>::min();
         c < std::numeric_limits<int8_t>::max();
         c++) {
        if (specials.find(c) != specials.end()) {
            EXPECT_TRUE(milvus::is_special(c)) << c << static_cast<int>(c);
        } else {
            EXPECT_FALSE(milvus::is_special(c)) << c << static_cast<int>(c);
        }
    }
}

TEST(TranslatePatternMatchToRegexTest, SimplePatternWithPercent) {
    std::string pattern = "abc%";
    std::string result = milvus::translate_pattern_match_to_regex(pattern);
    EXPECT_EQ(result, "abc[\\s\\S]*");
}

TEST(TranslatePatternMatchToRegexTest, PatternWithUnderscore) {
    std::string pattern = "a_c";
    std::string result = milvus::translate_pattern_match_to_regex(pattern);
    EXPECT_EQ(result, "a[\\s\\S]c");
}

TEST(TranslatePatternMatchToRegexTest, PatternWithSpecialCharacters) {
    std::string pattern = "a\\%b\\_c";
    std::string result = milvus::translate_pattern_match_to_regex(pattern);
    EXPECT_EQ(result, "a%b_c");
}

TEST(TranslatePatternMatchToRegexTest,
     PatternWithMultiplePercentAndUnderscore) {
    std::string pattern = "%a_b%";
    std::string result = milvus::translate_pattern_match_to_regex(pattern);
    EXPECT_EQ(result, "[\\s\\S]*a[\\s\\S]b[\\s\\S]*");
}

TEST(TranslatePatternMatchToRegexTest, PatternWithRegexChar) {
    std::string pattern = "abc*def.ghi+";
    std::string result = milvus::translate_pattern_match_to_regex(pattern);
    EXPECT_EQ(result, "abc\\*def\\.ghi\\+");
}

TEST(TranslatePatternMatchToRegexTest, MixPattern) {
    std::string pattern = R"(abc\+\def%ghi_[\\)";
    std::string result = milvus::translate_pattern_match_to_regex(pattern);
    EXPECT_EQ(result, R"(abc\+def[\s\S]*ghi[\s\S]\[\\)");
}

TEST(PatternMatchTranslatorTest, InvalidTypeTest) {
    using namespace milvus;
    PatternMatchTranslator translator;

    ASSERT_ANY_THROW(translator(123));
    ASSERT_ANY_THROW(translator(3.14));
    ASSERT_ANY_THROW(translator(true));
}

TEST(PatternMatchTranslatorTest, StringTypeTest) {
    using namespace milvus;
    PatternMatchTranslator translator;

    std::string pattern1 = "abc";
    std::string pattern2 = "xyz";
    std::string pattern3 = "%a_b%";

    EXPECT_EQ(translator(pattern1), "abc");
    EXPECT_EQ(translator(pattern2), "xyz");
    EXPECT_EQ(translator(pattern3), "[\\s\\S]*a[\\s\\S]b[\\s\\S]*");
}

TEST(PatternMatchTranslatorTest, TrailingBackslashIsError) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Trailing backslash should throw an error
    EXPECT_ANY_THROW(translator(std::string("abc\\")));
    EXPECT_ANY_THROW(translator(std::string("\\")));
    EXPECT_ANY_THROW(translator(std::string("%\\")));

    // Valid escape sequences should work
    EXPECT_NO_THROW(translator(std::string("\\%")));
    EXPECT_NO_THROW(translator(std::string("\\\\")));
}

TEST(RegexMatcherTest, DefaultBehaviorTest) {
    using namespace milvus;
    std::string pattern("Hello.*");
    RegexMatcher matcher(pattern);

    int operand1 = 123;
    double operand2 = 3.14;
    bool operand3 = true;

    EXPECT_FALSE(matcher(operand1));
    EXPECT_FALSE(matcher(operand2));
    EXPECT_FALSE(matcher(operand3));
}

TEST(RegexMatcherTest, StringMatchTest) {
    using namespace milvus;
    std::string pattern("Hello.*");
    RegexMatcher matcher(pattern);

    std::string str1 = "Hello, World!";
    std::string str2 = "Hi there!";
    std::string str3 = "Hello, OpenAI!";

    EXPECT_TRUE(matcher(str1));
    EXPECT_FALSE(matcher(str2));
    EXPECT_TRUE(matcher(str3));
}

TEST(RegexMatcherTest, StringViewMatchTest) {
    using namespace milvus;
    std::string pattern("Hello.*");
    RegexMatcher matcher(pattern);

    std::string_view str1 = "Hello, World!";
    std::string_view str2 = "Hi there!";
    std::string_view str3 = "Hello, OpenAI!";

    EXPECT_TRUE(matcher(str1));
    EXPECT_FALSE(matcher(str2));
    EXPECT_TRUE(matcher(str3));
}

TEST(RegexMatcherTest, NewLine) {
    GTEST_SKIP() << "TODO: matching behavior on newline";

    using namespace milvus;
    std::string pattern("Hello.*");
    RegexMatcher matcher(pattern);

    EXPECT_FALSE(matcher(std::string("Hello\n")));
}

TEST(RegexMatcherTest, PatternMatchWithNewLine) {
    using namespace milvus;
    std::string pattern("Hello%");
    PatternMatchTranslator translator;
    auto rp = translator(pattern);
    RegexMatcher matcher(rp);

    EXPECT_TRUE(matcher(std::string("Hello\n")));
}

// ============== extract_fixed_prefix_from_pattern Tests ==============

TEST(ExtractFixedPrefixTest, SimplePrefix) {
    using namespace milvus;
    // Pattern "abc%" -> prefix "abc"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("abc%"), "abc");
    // Pattern "abc%def" -> prefix "abc"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("abc%def"), "abc");
    // Pattern "hello%world%" -> prefix "hello"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("hello%world%"), "hello");
}

TEST(ExtractFixedPrefixTest, UnderscoreWildcard) {
    using namespace milvus;
    // Pattern "a_c" -> prefix "a" (stops at _)
    EXPECT_EQ(extract_fixed_prefix_from_pattern("a_c"), "a");
    // Pattern "ab_cd%" -> prefix "ab"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("ab_cd%"), "ab");
    // Pattern "_abc" -> prefix "" (starts with _)
    EXPECT_EQ(extract_fixed_prefix_from_pattern("_abc"), "");
}

TEST(ExtractFixedPrefixTest, NoPrefix) {
    using namespace milvus;
    // Pattern "%abc" -> prefix ""
    EXPECT_EQ(extract_fixed_prefix_from_pattern("%abc"), "");
    // Pattern "%abc%" -> prefix ""
    EXPECT_EQ(extract_fixed_prefix_from_pattern("%abc%"), "");
    // Pattern "%" -> prefix ""
    EXPECT_EQ(extract_fixed_prefix_from_pattern("%"), "");
    // Pattern "_" -> prefix ""
    EXPECT_EQ(extract_fixed_prefix_from_pattern("_"), "");
}

TEST(ExtractFixedPrefixTest, EscapedPercent) {
    using namespace milvus;
    // Pattern "100\%" -> prefix "100%" (escaped % is literal)
    EXPECT_EQ(extract_fixed_prefix_from_pattern("100\\%"), "100%");
    // Pattern "a\%b%" -> prefix "a%b"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("a\\%b%"), "a%b");
    // Pattern "100\%\%" -> prefix "100%%"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("100\\%\\%"), "100%%");
}

TEST(ExtractFixedPrefixTest, EscapedUnderscore) {
    using namespace milvus;
    // Pattern "a\_b" -> prefix "a_b" (escaped _ is literal)
    EXPECT_EQ(extract_fixed_prefix_from_pattern("a\\_b"), "a_b");
    // Pattern "a\_b%" -> prefix "a_b"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("a\\_b%"), "a_b");
    // Pattern "a\_b_c" -> prefix "a_b" (stops at unescaped _)
    EXPECT_EQ(extract_fixed_prefix_from_pattern("a\\_b_c"), "a_b");
}

TEST(ExtractFixedPrefixTest, MixedEscape) {
    using namespace milvus;
    // Pattern "10\%\_off%" -> prefix "10%_off"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("10\\%\\_off%"), "10%_off");
    // Pattern "a\%b\_c%d" -> prefix "a%b_c"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("a\\%b\\_c%d"), "a%b_c");
}

TEST(ExtractFixedPrefixTest, NoWildcard) {
    using namespace milvus;
    // Pattern "abc" -> prefix "abc" (no wildcard)
    EXPECT_EQ(extract_fixed_prefix_from_pattern("abc"), "abc");
    // Pattern "hello world" -> prefix "hello world"
    EXPECT_EQ(extract_fixed_prefix_from_pattern("hello world"), "hello world");
}

TEST(ExtractFixedPrefixTest, EmptyPattern) {
    using namespace milvus;
    // Empty pattern -> empty prefix
    EXPECT_EQ(extract_fixed_prefix_from_pattern(""), "");
}

TEST(ExtractFixedPrefixTest, TrailingBackslashIsError) {
    using namespace milvus;
    // Trailing backslash should throw an error
    EXPECT_ANY_THROW(extract_fixed_prefix_from_pattern("abc\\"));
    EXPECT_ANY_THROW(extract_fixed_prefix_from_pattern("\\"));

    // Trailing backslash after wildcard is also an error
    // (even though we stop at wildcard, trailing backslash at end of pattern is invalid)
    // Note: "abc%\\" has wildcard first, so prefix is "abc" and we don't see trailing backslash
    // But "abc\\" has no wildcard, so trailing backslash is reached
    EXPECT_ANY_THROW(extract_fixed_prefix_from_pattern("test\\"));

    // Valid escape sequences should work
    EXPECT_NO_THROW(extract_fixed_prefix_from_pattern("\\%"));
    EXPECT_NO_THROW(extract_fixed_prefix_from_pattern("\\\\"));
    EXPECT_NO_THROW(extract_fixed_prefix_from_pattern("abc\\%def"));
}

// ============== RE2 vs Boost::Regex Correctness Tests ==============
// These tests verify that RE2 (RegexMatcher) produces identical results
// to boost::regex (BoostRegexMatcher) for various patterns

TEST(RegexMatcherCorrectnessTest, SimplePatternComparison) {
    using namespace milvus;
    std::string pattern("Hello.*");
    RegexMatcher re2_matcher(pattern);
    BoostRegexMatcher boost_matcher(pattern);

    std::vector<std::string> test_strings = {
        "Hello, World!",
        "Hi there!",
        "Hello, OpenAI!",
        "Hello",
        "HelloWorld",
        "hello",  // case sensitive
        "",
        "Hello\nWorld",
    };

    for (const auto& str : test_strings) {
        EXPECT_EQ(re2_matcher(str), boost_matcher(str))
            << "Pattern: " << pattern << ", String: " << str;
    }
}

TEST(RegexMatcherCorrectnessTest, LikePatternComparison) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test various LIKE patterns
    std::vector<std::string> like_patterns = {
        "abc%",     // prefix match
        "%abc",     // suffix match
        "%abc%",    // inner match
        "a%b%c",    // complex pattern
        "a_c",      // single char wildcard
        "a__c",     // multiple single char wildcards
        "%",        // match all
        "abc",      // exact match
        "a%b_c%d",  // mixed wildcards
    };

    std::vector<std::string> test_strings = {
        "abc",
        "abcdef",
        "xyzabc",
        "xyzabcdef",
        "aXc",
        "aXYc",
        "abc123def",
        "a1b2c",
        "a1b2c3d",
        "aXbYcZd",
        "",
        "a",
        "ab",
    };

    for (const auto& like_pattern : like_patterns) {
        std::string regex_pattern = translator(like_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);

        for (const auto& str : test_strings) {
            EXPECT_EQ(re2_matcher(str), boost_matcher(str))
                << "LIKE: " << like_pattern << ", Regex: " << regex_pattern
                << ", String: " << str;
        }
    }
}

TEST(RegexMatcherCorrectnessTest, StringViewComparison) {
    using namespace milvus;
    PatternMatchTranslator translator;
    std::string like_pattern = "%hello%";
    std::string regex_pattern = translator(like_pattern);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);

    std::vector<std::string> test_strings = {
        "hello world",
        "say hello there",
        "HELLO",  // case sensitive - should not match
        "he11o",  // not match
        "hello",
        "",
    };

    for (const auto& str : test_strings) {
        std::string_view sv(str);
        EXPECT_EQ(re2_matcher(sv), boost_matcher(sv))
            << "Pattern: " << like_pattern << ", String: " << str;
    }
}

TEST(RegexMatcherCorrectnessTest, NewlineHandling) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // LIKE patterns should match across newlines
    std::vector<std::pair<std::string, std::string>> test_cases = {
        {"hello%", "hello\nworld"},
        {"%world", "hello\nworld"},
        {"hello%world", "hello\nworld"},
        {"%", "line1\nline2\nline3"},
        {"a_b", "a\nb"},
    };

    for (const auto& [like_pattern, test_string] : test_cases) {
        std::string regex_pattern = translator(like_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);

        EXPECT_EQ(re2_matcher(test_string), boost_matcher(test_string))
            << "Pattern: " << like_pattern << ", String: (contains newline)";
    }
}

TEST(RegexMatcherCorrectnessTest, SpecialCharacters) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test patterns with escaped special characters
    std::vector<std::pair<std::string, std::vector<std::string>>> test_cases = {
        // Pattern with escaped %
        {"100\\%%", {"100%discount", "100%", "100"}},
        // Pattern with escaped _
        {"file\\_name%", {"file_name.txt", "file_name", "filename"}},
        // Pattern with regex special chars
        {"test.%", {"test.cpp", "test.java", "testXcpp"}},
        {"(test)%", {"(test)", "(test)abc", "test"}},
        {"[test]%", {"[test]", "[test]abc", "test"}},
    };

    for (const auto& [like_pattern, test_strings] : test_cases) {
        std::string regex_pattern = translator(like_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);

        for (const auto& str : test_strings) {
            EXPECT_EQ(re2_matcher(str), boost_matcher(str))
                << "Pattern: " << like_pattern << ", String: " << str;
        }
    }
}

TEST(RegexMatcherCorrectnessTest, LongStrings) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Generate long strings
    std::string long_prefix(500, 'a');
    std::string long_suffix(500, 'z');
    std::string long_middle = long_prefix + "NEEDLE" + long_suffix;

    std::string like_pattern = "%NEEDLE%";
    std::string regex_pattern = translator(like_pattern);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);

    std::vector<std::string> test_strings = {
        long_middle,
        long_prefix,  // no needle
        long_suffix,  // no needle
        "NEEDLE",
        "xNEEDLEx",
    };

    for (const auto& str : test_strings) {
        EXPECT_EQ(re2_matcher(str), boost_matcher(str))
            << "Pattern: " << like_pattern << ", String length: " << str.size();
    }
}

TEST(BoostRegexMatcherTest, BasicFunctionality) {
    using namespace milvus;
    std::string pattern("Hello.*");
    BoostRegexMatcher matcher(pattern);

    EXPECT_TRUE(matcher(std::string("Hello, World!")));
    EXPECT_FALSE(matcher(std::string("Hi there!")));
    EXPECT_TRUE(matcher(std::string("Hello")));

    // Test string_view
    std::string_view sv = "Hello, World!";
    EXPECT_TRUE(matcher(sv));
}

TEST(BoostRegexMatcherTest, DefaultBehavior) {
    using namespace milvus;
    std::string pattern("Test.*");
    BoostRegexMatcher matcher(pattern);

    // Non-string types should return false
    EXPECT_FALSE(matcher(123));
    EXPECT_FALSE(matcher(3.14));
    EXPECT_FALSE(matcher(true));
}

// ============== MultiWildcardMatcher Tests ==============

TEST(MultiWildcardMatcherTest, SimplePatterns) {
    using namespace milvus;

    // Test "abc" (exact match)
    {
        MultiWildcardMatcher matcher("abc");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_FALSE(matcher(std::string("abcd")));
        EXPECT_FALSE(matcher(std::string("xabc")));
        EXPECT_FALSE(matcher(std::string("")));
    }

    // Test "abc%" (prefix match)
    {
        MultiWildcardMatcher matcher("abc%");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("abcdef")));
        EXPECT_FALSE(matcher(std::string("xabc")));
        EXPECT_FALSE(matcher(std::string("ab")));
    }

    // Test "%abc" (suffix match)
    {
        MultiWildcardMatcher matcher("%abc");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("xyzabc")));
        EXPECT_FALSE(matcher(std::string("abcx")));
        EXPECT_FALSE(matcher(std::string("ab")));
    }

    // Test "%abc%" (contains)
    {
        MultiWildcardMatcher matcher("%abc%");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("xabcy")));
        EXPECT_TRUE(matcher(std::string("abcdef")));
        EXPECT_TRUE(matcher(std::string("xyzabc")));
        EXPECT_FALSE(matcher(std::string("ab")));
        EXPECT_FALSE(matcher(std::string("axbxc")));
    }
}

TEST(MultiWildcardMatcherTest, ComplexPatterns) {
    using namespace milvus;

    // Test "a%b%c" (multi-segment)
    {
        MultiWildcardMatcher matcher("a%b%c");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("aXbYc")));
        EXPECT_TRUE(matcher(std::string("aXXXbYYYc")));
        EXPECT_FALSE(matcher(std::string("Xabc")));  // must start with 'a'
        EXPECT_FALSE(matcher(std::string("abcX")));  // must end with 'c'
        EXPECT_FALSE(matcher(std::string("acb")));   // wrong order
        EXPECT_FALSE(matcher(std::string("ab")));    // missing 'c'
    }

    // Test "%a%b%c%" (multi-segment with wildcards at both ends)
    {
        MultiWildcardMatcher matcher("%a%b%c%");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("XaYbZcW")));
        EXPECT_TRUE(matcher(std::string("aXbYc")));
        EXPECT_FALSE(matcher(std::string("acb")));  // wrong order
    }

    // Test "hello%world" (prefix and suffix)
    {
        MultiWildcardMatcher matcher("hello%world");
        EXPECT_TRUE(matcher(std::string("helloworld")));
        EXPECT_TRUE(matcher(std::string("hello beautiful world")));
        EXPECT_FALSE(matcher(std::string("hello")));
        EXPECT_FALSE(matcher(std::string("world")));
        EXPECT_FALSE(matcher(std::string("Xhelloworld")));
        EXPECT_FALSE(matcher(std::string("helloworldX")));
    }
}

TEST(MultiWildcardMatcherTest, EscapedCharacters) {
    using namespace milvus;

    // Test with escaped % followed by wildcard
    {
        MultiWildcardMatcher matcher("100\\%%");
        EXPECT_TRUE(matcher(std::string("100%")));
        EXPECT_TRUE(matcher(std::string("100%discount")));
        EXPECT_FALSE(matcher(std::string("100")));
        EXPECT_FALSE(matcher(std::string("100X")));
    }

    // Test with trailing escaped % (no wildcard after) - critical regression test
    {
        MultiWildcardMatcher matcher("100\\%");
        EXPECT_TRUE(matcher(std::string("100%")));
        EXPECT_FALSE(matcher(std::string("100")));
        EXPECT_FALSE(matcher(std::string("100%X")));  // Must not allow suffix!
        EXPECT_FALSE(matcher(std::string("X100%")));
    }

    // Test with escaped _
    {
        MultiWildcardMatcher matcher("file\\_name");
        EXPECT_TRUE(matcher(std::string("file_name")));
        EXPECT_FALSE(matcher(std::string("fileXname")));
        EXPECT_FALSE(matcher(std::string("file_name_")));
    }

    // Test leading escaped %
    {
        MultiWildcardMatcher matcher("\\%value");
        EXPECT_TRUE(matcher(std::string("%value")));
        EXPECT_FALSE(matcher(std::string("value")));
        EXPECT_FALSE(matcher(std::string("X%value")));
    }
}

TEST(MultiWildcardMatcherTest, StringViewSupport) {
    using namespace milvus;

    MultiWildcardMatcher matcher("%hello%");
    std::string_view sv1 = "hello world";
    std::string_view sv2 = "world";

    EXPECT_TRUE(matcher(sv1));
    EXPECT_FALSE(matcher(sv2));
}

// ============== SmartPatternMatcher Tests ==============

TEST(SmartPatternMatcherTest, UsesMultiWildcardForSimplePatterns) {
    using namespace milvus;

    // Pattern with only % wildcards
    SmartPatternMatcher matcher("a%b%c");
    EXPECT_TRUE(matcher(std::string("abc")));
    EXPECT_TRUE(matcher(std::string("aXbYc")));
    EXPECT_FALSE(matcher(std::string("Xabc")));
}

TEST(SmartPatternMatcherTest, HandlesUnderscorePatterns) {
    using namespace milvus;

    // Pattern with _ wildcard should still match correctly
    SmartPatternMatcher matcher("a_c");
    EXPECT_TRUE(matcher(std::string("abc")));
    EXPECT_TRUE(matcher(std::string("aXc")));
    EXPECT_FALSE(matcher(std::string("ac")));
    EXPECT_FALSE(matcher(std::string("aXXc")));
}

TEST(SmartPatternMatcherTest, CorrectnessComparisonWithRegex) {
    using namespace milvus;

    // Test that SmartPatternMatcher gives same results as RegexMatcher
    std::vector<std::string> patterns = {
        "a%b%c",
        "%hello%",
        "world%",
        "%world",
        "a%b%c%d%e",
        "prefix%middle%suffix",
    };

    std::vector<std::string> test_strings = {
        "abc",
        "aXbYc",
        "hello",
        "say hello there",
        "world",
        "world domination",
        "hello world",
        "aXbXcXdXe",
        "prefixXmiddleYsuffix",
        "",
        "random string",
    };

    PatternMatchTranslator translator;
    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& str : test_strings) {
            EXPECT_EQ(smart_matcher(str), regex_matcher(str))
                << "Pattern: " << pattern << ", String: " << str;
        }
    }
}

// ============== Escaped Pattern Tests ==============
// These tests verify correct handling of escaped % and _ characters

TEST(EscapedPatternTest, TrailingEscapedPercent) {
    using namespace milvus;

    // Pattern "100\\%" should match exactly "100%" (literal percent at end)
    {
        LikePatternMatcher matcher("100\\%");
        EXPECT_TRUE(matcher(std::string("100%")));
        EXPECT_FALSE(matcher(std::string("100")));
        EXPECT_FALSE(matcher(std::string("100%extra")));
        EXPECT_FALSE(matcher(std::string("100X")));
        EXPECT_FALSE(matcher(std::string("X100%")));
    }

    // Pattern "a%\\%" should match strings starting with 'a' and ending with '%'
    {
        LikePatternMatcher matcher("a%\\%");
        EXPECT_TRUE(matcher(std::string("a%")));
        EXPECT_TRUE(matcher(std::string("abc%")));
        EXPECT_TRUE(matcher(std::string("a123%")));
        EXPECT_FALSE(matcher(std::string("a")));
        EXPECT_FALSE(matcher(std::string("abc")));
        EXPECT_FALSE(matcher(std::string("a%extra")));
        EXPECT_FALSE(matcher(std::string("b%")));
    }

    // Pattern "%\\%" should match any string ending with '%'
    {
        LikePatternMatcher matcher("%\\%");
        EXPECT_TRUE(matcher(std::string("%")));
        EXPECT_TRUE(matcher(std::string("100%")));
        EXPECT_TRUE(matcher(std::string("discount 50%")));
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_FALSE(matcher(std::string("no percent")));
        EXPECT_FALSE(matcher(std::string("%extra")));
    }

    // Pattern "\\%\\%" should match exactly "%%"
    {
        LikePatternMatcher matcher("\\%\\%");
        EXPECT_TRUE(matcher(std::string("%%")));
        EXPECT_FALSE(matcher(std::string("%")));
        EXPECT_FALSE(matcher(std::string("%%%")));
        EXPECT_FALSE(matcher(std::string("X%%")));
    }
}

TEST(EscapedPatternTest, LeadingEscapedPercent) {
    using namespace milvus;

    // Pattern "\\%100" should match exactly "%100"
    {
        LikePatternMatcher matcher("\\%100");
        EXPECT_TRUE(matcher(std::string("%100")));
        EXPECT_FALSE(matcher(std::string("100")));
        EXPECT_FALSE(matcher(std::string("X%100")));
        EXPECT_FALSE(matcher(std::string("%100X")));
    }

    // Pattern "\\%%" should match strings starting with '%'
    {
        LikePatternMatcher matcher("\\%%");
        EXPECT_TRUE(matcher(std::string("%")));
        EXPECT_TRUE(matcher(std::string("%abc")));
        EXPECT_TRUE(matcher(std::string("%100")));
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_FALSE(matcher(std::string("no percent")));
        EXPECT_FALSE(matcher(std::string("abc%")));
    }
}

TEST(EscapedPatternTest, EscapedUnderscore) {
    using namespace milvus;

    // Pattern "a\\_b" should match exactly "a_b"
    {
        LikePatternMatcher matcher("a\\_b");
        EXPECT_TRUE(matcher(std::string("a_b")));
        EXPECT_FALSE(matcher(std::string("aXb")));
        EXPECT_FALSE(matcher(std::string("ab")));
        EXPECT_FALSE(matcher(std::string("a_b_")));
    }

    // Pattern "file\\_name%" should match "file_name" prefix
    {
        LikePatternMatcher matcher("file\\_name%");
        EXPECT_TRUE(matcher(std::string("file_name")));
        EXPECT_TRUE(matcher(std::string("file_name.txt")));
        EXPECT_FALSE(matcher(std::string("fileXname")));
        EXPECT_FALSE(matcher(std::string("file_")));
    }

    // Pattern "%\\_%" should match strings containing '_'
    {
        LikePatternMatcher matcher("%\\_%");
        EXPECT_TRUE(matcher(std::string("_")));
        EXPECT_TRUE(matcher(std::string("a_b")));
        EXPECT_TRUE(matcher(std::string("_start")));
        EXPECT_TRUE(matcher(std::string("end_")));
        EXPECT_FALSE(matcher(std::string("no underscore")));
    }
}

TEST(EscapedPatternTest, MixedEscapedAndWildcard) {
    using namespace milvus;

    // Pattern "\\%%\\%" should match strings starting and ending with '%'
    {
        LikePatternMatcher matcher("\\%%\\%");
        EXPECT_TRUE(matcher(std::string("%%")));
        EXPECT_TRUE(matcher(std::string("%abc%")));
        EXPECT_TRUE(matcher(std::string("%X%")));
        EXPECT_FALSE(matcher(std::string("%")));
        EXPECT_FALSE(matcher(std::string("abc%")));
        EXPECT_FALSE(matcher(std::string("%abc")));
    }

    // Pattern "a_\\%b" should match 'a' + any single char + '%' + 'b'
    {
        LikePatternMatcher matcher("a_\\%b");
        EXPECT_TRUE(matcher(std::string("aX%b")));
        EXPECT_TRUE(matcher(std::string("a1%b")));
        EXPECT_FALSE(matcher(std::string("a%b")));
        EXPECT_FALSE(matcher(std::string("aXXb")));
        EXPECT_FALSE(matcher(std::string("aX%bX")));
    }

    // Pattern "test\\_%\\_end" should match "test_...._end"
    {
        LikePatternMatcher matcher("test\\_%\\_end");
        EXPECT_TRUE(matcher(std::string("test__end")));
        EXPECT_TRUE(matcher(std::string("test_abc_end")));
        EXPECT_FALSE(matcher(std::string("test_end")));
        EXPECT_FALSE(matcher(std::string("testX_end")));
    }
}

TEST(EscapedPatternTest, CorrectnessComparisonWithRegex) {
    using namespace milvus;

    // Test that SmartPatternMatcher gives same results as RegexMatcher for escaped patterns
    std::vector<std::string> patterns = {
        "100\\%",          // trailing escaped %
        "a%\\%",           // wildcard then escaped %
        "%\\%",            // wildcard ending with escaped %
        "\\%100",          // leading escaped %
        "\\%%",            // escaped % then wildcard
        "\\%\\%",          // two escaped %
        "a\\_b",           // escaped underscore
        "file\\_name%",    // escaped underscore with wildcard
        "%\\_%",           // wildcard with escaped underscore
        "\\%%\\%",         // escaped % + wildcard + escaped %
        "a_\\%b",          // underscore and escaped %
        "test\\_%\\_end",  // complex mixed pattern
        "100\\%\\%",       // number with two escaped %
        "\\%\\_%",         // escaped % + escaped _ + wildcard
    };

    std::vector<std::string> test_strings = {
        "100%",      "100",          "100%extra",     "a%",    "abc%",
        "%",         "%%",           "%100",          "%abc",  "a_b",
        "aXb",       "file_name",    "file_name.txt", "_",     "a_b_c",
        "test__end", "test_abc_end", "aX%b",          "100%%", "%_%",
        "",          "random",
    };

    PatternMatchTranslator translator;
    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& str : test_strings) {
            bool regex_result = regex_matcher(str);
            bool smart_result = smart_matcher(str);
            EXPECT_EQ(smart_result, regex_result)
                << "Pattern: \"" << pattern << "\", String: \"" << str << "\""
                << ", Regex: " << regex_result << ", Smart: " << smart_result;
        }
    }
}

TEST(EscapedPatternTest, EscapedBackslash) {
    using namespace milvus;

    // Pattern with escaped backslash (\\\\) should match literal backslash
    // Note: In C++ string, \\\\ becomes \\ which represents one backslash in the pattern
    {
        LikePatternMatcher matcher("a\\\\b");
        EXPECT_TRUE(matcher(std::string("a\\b")));
        EXPECT_FALSE(matcher(std::string("ab")));
        EXPECT_FALSE(matcher(std::string("a\\\\b")));
    }

    // Pattern "path\\\\%" should match "path\" followed by anything
    {
        LikePatternMatcher matcher("path\\\\%");
        EXPECT_TRUE(matcher(std::string("path\\")));
        EXPECT_TRUE(matcher(std::string("path\\file")));
        EXPECT_FALSE(matcher(std::string("path")));
        EXPECT_FALSE(matcher(std::string("pathX")));
    }
}

// ============== BREAKING CHANGE: Trailing Backslash Now Throws Error ==============
// BEFORE this PR: A trailing backslash was silently ignored.
//   - Pattern "abc\" would be treated as "abc" (escape_mode left hanging)
//   - This was undefined/buggy behavior
// AFTER this PR: A trailing backslash throws ExprInvalid error.
//   - Pattern "abc\" now throws "Invalid LIKE pattern: trailing backslash with nothing to escape"
//   - This is the correct SQL LIKE behavior
//
// This affects ALL pattern parsing components:
//   - translate_pattern_match_to_regex()
//   - extract_fixed_prefix_from_pattern()
//   - LikePatternMatcher
//   - SmartPatternMatcher

TEST(EscapedPatternTest, TrailingBackslashIsError) {
    using namespace milvus;

    // ===== translate_pattern_match_to_regex =====
    // BREAKING: Previously returned incomplete regex, now throws
    PatternMatchTranslator translator;
    EXPECT_ANY_THROW(translator(std::string("abc\\")));
    EXPECT_ANY_THROW(translator(std::string("\\")));
    EXPECT_ANY_THROW(translator(std::string("%\\")));

    // ===== extract_fixed_prefix_from_pattern =====
    // BREAKING: Previously returned prefix ignoring trailing backslash, now throws
    EXPECT_ANY_THROW(extract_fixed_prefix_from_pattern("abc\\"));
    EXPECT_ANY_THROW(extract_fixed_prefix_from_pattern("\\"));
    // Note: "abc%\\" stops at % first, so prefix is "abc" and doesn't see trailing backslash
    // But patterns without wildcard before trailing backslash will throw
    EXPECT_ANY_THROW(extract_fixed_prefix_from_pattern("test\\"));

    // ===== LikePatternMatcher =====
    // BREAKING: Previously had undefined behavior, now throws
    EXPECT_ANY_THROW(LikePatternMatcher("abc\\"));
    EXPECT_ANY_THROW(LikePatternMatcher("\\"));
    EXPECT_ANY_THROW(LikePatternMatcher("%\\"));
    EXPECT_ANY_THROW(LikePatternMatcher("_\\"));
    EXPECT_ANY_THROW(LikePatternMatcher("a%b\\"));

    // ===== SmartPatternMatcher =====
    // BREAKING: Previously had undefined behavior, now throws
    EXPECT_ANY_THROW(SmartPatternMatcher("abc\\"));
    EXPECT_ANY_THROW(SmartPatternMatcher("\\"));
    EXPECT_ANY_THROW(SmartPatternMatcher("%\\"));
}

TEST(EscapedPatternTest, ValidEscapeSequences) {
    using namespace milvus;

    // These should NOT throw - valid escape sequences
    EXPECT_NO_THROW(LikePatternMatcher("\\%"));      // escaped %
    EXPECT_NO_THROW(LikePatternMatcher("\\_"));      // escaped _
    EXPECT_NO_THROW(LikePatternMatcher("\\\\"));     // escaped backslash
    EXPECT_NO_THROW(LikePatternMatcher("abc\\%"));   // text + escaped %
    EXPECT_NO_THROW(LikePatternMatcher("abc\\\\"));  // text + escaped backslash
    EXPECT_NO_THROW(
        LikePatternMatcher("%\\%%"));  // wildcard + escaped % + wildcard
}

// ============== Regex Metacharacter Tests ==============
// These tests verify that LIKE patterns containing regex metacharacters
// (., [], (), etc.) are properly escaped and match literally

TEST(SmartPatternMatcherTest, RegexMetacharacterDot) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "a.b" should match literal "a.b", not "aXb"
    {
        std::string pattern = "a.b";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("a.b")));
        EXPECT_TRUE(regex_matcher(std::string("a.b")));
        // Dot in LIKE is literal, not wildcard
        EXPECT_FALSE(smart_matcher(std::string("aXb")));
        EXPECT_FALSE(regex_matcher(std::string("aXb")));
    }

    // Pattern "%.%" should match strings containing literal dot
    {
        std::string pattern = "%.%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("file.txt")));
        EXPECT_TRUE(regex_matcher(std::string("file.txt")));
        EXPECT_TRUE(smart_matcher(std::string("a.b")));
        EXPECT_TRUE(regex_matcher(std::string("a.b")));
        EXPECT_FALSE(smart_matcher(std::string("noperiod")));
        EXPECT_FALSE(regex_matcher(std::string("noperiod")));
    }
}

TEST(SmartPatternMatcherTest, RegexMetacharacterBrackets) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "[test]" should match literal "[test]"
    {
        std::string pattern = "[test]";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("[test]")));
        EXPECT_TRUE(regex_matcher(std::string("[test]")));
        // Should NOT match single char from character class
        EXPECT_FALSE(smart_matcher(std::string("t")));
        EXPECT_FALSE(regex_matcher(std::string("t")));
    }

    // Pattern "%[%]%" should match strings containing "[" and "]"
    {
        std::string pattern = "%[%]%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("[x]")));
        EXPECT_TRUE(regex_matcher(std::string("[x]")));
        EXPECT_TRUE(smart_matcher(std::string("array[0]")));
        EXPECT_TRUE(regex_matcher(std::string("array[0]")));
    }
}

TEST(SmartPatternMatcherTest, RegexMetacharacterParentheses) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "(test)" should match literal "(test)"
    {
        std::string pattern = "(test)";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("(test)")));
        EXPECT_TRUE(regex_matcher(std::string("(test)")));
        EXPECT_FALSE(smart_matcher(std::string("test")));
        EXPECT_FALSE(regex_matcher(std::string("test")));
    }

    // Pattern "func(%)%" should match function-like patterns
    {
        std::string pattern = "func(%)%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("func()")));
        EXPECT_TRUE(regex_matcher(std::string("func()")));
        EXPECT_TRUE(smart_matcher(std::string("func(x)")));
        EXPECT_TRUE(regex_matcher(std::string("func(x)")));
        EXPECT_TRUE(smart_matcher(std::string("func(a,b)")));
        EXPECT_TRUE(regex_matcher(std::string("func(a,b)")));
    }
}

TEST(SmartPatternMatcherTest, RegexMetacharacterMixed) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test various regex metacharacters: ^ $ * + ? | { }
    std::vector<std::pair<std::string, std::vector<std::string>>> test_cases = {
        // Pattern with ^
        {"^start%", {{"^start", true}, {"^startXXX", true}, {"start", false}}},
        // Pattern with $
        {"%end$", {{"end$", true}, {"XXXend$", true}, {"end", false}}},
        // Pattern with *
        {"a*b", {{"a*b", true}, {"ab", false}, {"aab", false}}},
        // Pattern with +
        {"a+b", {{"a+b", true}, {"ab", false}, {"aab", false}}},
        // Pattern with ?
        {"a?b", {{"a?b", true}, {"ab", false}, {"b", false}}},
        // Pattern with |
        {"a|b", {{"a|b", true}, {"a", false}, {"b", false}}},
        // Pattern with { }
        {"a{2}", {{"a{2}", true}, {"aa", false}}},
    };

    for (const auto& [pattern, cases] : test_cases) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& [test_str, expected] : cases) {
            EXPECT_EQ(smart_matcher(std::string(test_str)), expected)
                << "SmartPattern: " << pattern << ", String: " << test_str;
            EXPECT_EQ(regex_matcher(std::string(test_str)), expected)
                << "RegexPattern: " << pattern << ", String: " << test_str;
        }
    }
}

TEST(SmartPatternMatcherTest, RegexMetacharacterCorrectnessComparison) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Comprehensive test: patterns with regex metacharacters
    std::vector<std::string> patterns = {
        "file.txt",    // dot
        "%.%",         // dot with wildcards
        "[array]",     // brackets
        "%[%]%",       // brackets with wildcards
        "(group)",     // parentheses
        "func(%)",     // parentheses with wildcard
        "a^b",         // caret
        "a$b",         // dollar
        "a*b",         // asterisk
        "a+b",         // plus
        "a?b",         // question mark
        "a|b",         // pipe
        "a{b}",        // braces
        "path\\file",  // backslash (escaped)
        "test.*",      // dot and asterisk
        "[a-z]",       // character class syntax
        "(?:test)",    // non-capturing group syntax
        "\\d+",        // regex digit syntax (literal in LIKE)
    };

    std::vector<std::string> test_strings = {
        "file.txt", "data.csv", "[array]",    "arr[0]", "(group)", "func(x)",
        "a^b",      "a$b",      "a*b",        "ab",     "a+b",     "a?b",
        "a|b",      "a{b}",     "path\\file", "test.*", "[a-z]",   "(?:test)",
        "\\d+",     "123",      "",
    };

    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& str : test_strings) {
            bool regex_result = regex_matcher(str);
            bool smart_result = smart_matcher(str);
            EXPECT_EQ(smart_result, regex_result)
                << "Pattern: \"" << pattern << "\", String: \"" << str << "\""
                << ", Regex: " << regex_result << ", Smart: " << smart_result;
        }
    }
}

// ============== LikePatternMatcher Comprehensive Tests ==============

TEST(LikePatternMatcherTest, EmptyPattern) {
    using namespace milvus;

    LikePatternMatcher matcher("");
    EXPECT_TRUE(matcher(std::string("")));
    EXPECT_FALSE(matcher(std::string("a")));
    EXPECT_FALSE(matcher(std::string(" ")));
}

TEST(LikePatternMatcherTest, OnlyPercentWildcard) {
    using namespace milvus;

    // Single %
    {
        LikePatternMatcher matcher("%");
        EXPECT_TRUE(matcher(std::string("")));
        EXPECT_TRUE(matcher(std::string("a")));
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("anything at all")));
    }

    // Multiple %
    {
        LikePatternMatcher matcher("%%");
        EXPECT_TRUE(matcher(std::string("")));
        EXPECT_TRUE(matcher(std::string("abc")));
    }

    // Three %
    {
        LikePatternMatcher matcher("%%%");
        EXPECT_TRUE(matcher(std::string("")));
        EXPECT_TRUE(matcher(std::string("abc")));
    }
}

TEST(LikePatternMatcherTest, OnlyUnderscoreWildcard) {
    using namespace milvus;

    // Single _
    {
        LikePatternMatcher matcher("_");
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_TRUE(matcher(std::string("a")));
        EXPECT_TRUE(matcher(std::string("X")));
        EXPECT_FALSE(matcher(std::string("ab")));
    }

    // Double _
    {
        LikePatternMatcher matcher("__");
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_FALSE(matcher(std::string("a")));
        EXPECT_TRUE(matcher(std::string("ab")));
        EXPECT_TRUE(matcher(std::string("XY")));
        EXPECT_FALSE(matcher(std::string("abc")));
    }

    // Triple _
    {
        LikePatternMatcher matcher("___");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("123")));
        EXPECT_FALSE(matcher(std::string("ab")));
        EXPECT_FALSE(matcher(std::string("abcd")));
    }
}

TEST(LikePatternMatcherTest, UnderscoreAtDifferentPositions) {
    using namespace milvus;

    // _ at start
    {
        LikePatternMatcher matcher("_bc");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("Xbc")));
        EXPECT_FALSE(matcher(std::string("bc")));
        EXPECT_FALSE(matcher(std::string("abbc")));
    }

    // _ at end
    {
        LikePatternMatcher matcher("ab_");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("abX")));
        EXPECT_FALSE(matcher(std::string("ab")));
        EXPECT_FALSE(matcher(std::string("abcd")));
    }

    // _ in middle
    {
        LikePatternMatcher matcher("a_c");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("aXc")));
        EXPECT_FALSE(matcher(std::string("ac")));
        EXPECT_FALSE(matcher(std::string("abbc")));
    }

    // Multiple _ spread out
    {
        LikePatternMatcher matcher("_b_d_");
        EXPECT_TRUE(matcher(std::string("abcde")));
        EXPECT_TRUE(matcher(std::string("XbYdZ")));
        EXPECT_FALSE(matcher(std::string("abcd")));
        EXPECT_FALSE(matcher(std::string("abcdef")));
    }
}

TEST(LikePatternMatcherTest, MixedPercentAndUnderscore) {
    using namespace milvus;

    // %_
    {
        LikePatternMatcher matcher("%_");
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_TRUE(matcher(std::string("a")));
        EXPECT_TRUE(matcher(std::string("abc")));
    }

    // _%
    {
        LikePatternMatcher matcher("_%");
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_TRUE(matcher(std::string("a")));
        EXPECT_TRUE(matcher(std::string("abc")));
    }

    // %_%
    {
        LikePatternMatcher matcher("%_%");
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_TRUE(matcher(std::string("a")));
        EXPECT_TRUE(matcher(std::string("abc")));
    }

    // a%_b
    {
        LikePatternMatcher matcher("a%_b");
        EXPECT_TRUE(matcher(std::string("aXb")));
        EXPECT_TRUE(matcher(std::string("aXXXYb")));
        EXPECT_FALSE(matcher(std::string("ab")));
        EXPECT_FALSE(matcher(std::string("aXbc")));
    }

    // a_%_b
    {
        LikePatternMatcher matcher("a_%_b");
        EXPECT_TRUE(matcher(std::string("aXYb")));
        EXPECT_TRUE(matcher(std::string("aXYZb")));
        EXPECT_FALSE(matcher(std::string("aXb")));
        EXPECT_FALSE(matcher(std::string("ab")));
    }
}

TEST(LikePatternMatcherTest, ConsecutiveUnderscoresWithPercent) {
    using namespace milvus;

    // %__% (at least 2 chars)
    {
        LikePatternMatcher matcher("%__%");
        EXPECT_FALSE(matcher(std::string("")));
        EXPECT_FALSE(matcher(std::string("a")));
        EXPECT_TRUE(matcher(std::string("ab")));
        EXPECT_TRUE(matcher(std::string("abc")));
    }

    // a%__b (a, anything, 2 chars, b)
    {
        LikePatternMatcher matcher("a%__b");
        EXPECT_TRUE(matcher(std::string("aXYb")));
        EXPECT_TRUE(matcher(std::string("aZZXYb")));
        EXPECT_FALSE(matcher(std::string("aXb")));
        EXPECT_FALSE(matcher(std::string("ab")));
    }
}

TEST(LikePatternMatcherTest, NonStringTypes) {
    using namespace milvus;

    LikePatternMatcher matcher("test%");

    // Non-string types should return false
    EXPECT_FALSE(matcher(123));
    EXPECT_FALSE(matcher(3.14));
    EXPECT_FALSE(matcher(true));
    EXPECT_FALSE(matcher('c'));
}

TEST(LikePatternMatcherTest, StringViewSupport) {
    using namespace milvus;

    LikePatternMatcher matcher("%hello%");

    std::string_view sv1 = "hello";
    std::string_view sv2 = "say hello there";
    std::string_view sv3 = "world";
    std::string_view sv4 = "";

    EXPECT_TRUE(matcher(sv1));
    EXPECT_TRUE(matcher(sv2));
    EXPECT_FALSE(matcher(sv3));
    EXPECT_FALSE(matcher(sv4));
}

TEST(LikePatternMatcherTest, VeryLongStrings) {
    using namespace milvus;

    std::string long_str(10000, 'a');
    long_str += "needle";
    long_str += std::string(10000, 'b');

    LikePatternMatcher matcher("%needle%");
    EXPECT_TRUE(matcher(long_str));

    LikePatternMatcher no_match("%notfound%");
    EXPECT_FALSE(no_match(long_str));
}

TEST(LikePatternMatcherTest, VeryLongPattern) {
    using namespace milvus;

    std::string pattern = "a";
    for (int i = 0; i < 100; ++i) {
        pattern += "%b";
    }

    LikePatternMatcher matcher(pattern);

    std::string matching = "a";
    for (int i = 0; i < 100; ++i) {
        matching += "XXXb";
    }
    EXPECT_TRUE(matcher(matching));

    // Missing one 'b' segment
    EXPECT_FALSE(matcher(std::string("aXXXb")));
}

TEST(LikePatternMatcherTest, UnicodeStrings) {
    using namespace milvus;

    // Chinese characters
    {
        LikePatternMatcher matcher(
            "%\xE4\xBD\xA0\xE5\xA5\xBD%");  // 你好 in UTF-8
        EXPECT_TRUE(matcher(std::string(
            "\xE4\xBD\xA0\xE5\xA5\xBD\xE4\xB8\x96\xE7\x95\x8C")));  // 你好世界
        EXPECT_FALSE(matcher(std::string("\xE4\xB8\x96\xE7\x95\x8C")));  // 世界
    }

    // Mixed ASCII and UTF-8
    {
        LikePatternMatcher matcher("caf\xC3\xA9%");  // café
        EXPECT_TRUE(matcher(std::string("caf\xC3\xA9")));
        EXPECT_TRUE(matcher(std::string("caf\xC3\xA9 au lait")));
        EXPECT_FALSE(matcher(std::string("cafe")));
    }
}

// ============== Byte-Level Underscore Semantics Tests ==============
// IMPORTANT: The LikePatternMatcher's underscore '_' matches ONE BYTE, not one codepoint.
// This is because LikePatternMatcher operates at the byte level for performance.
//
// The regex translation uses [\s\S] which in some engines (like RE2 in UTF-8 mode)
// matches one codepoint, not one byte. This test suite verifies that all implementations
// (LikePatternMatcher, RE2, Boost) produce consistent results.
//
// If there's a mismatch between byte-level LikePatternMatcher and codepoint-level
// regex engines, these tests will expose it.

TEST(UnderscoreByteSemanticsTest, SingleByteCharacters) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // For ASCII (single-byte), _ matches exactly one character
    {
        LikePatternMatcher matcher("a_c");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("aXc")));
        EXPECT_FALSE(matcher(std::string("ac")));    // Too short
        EXPECT_FALSE(matcher(std::string("abbc")));  // Too long
    }
}

TEST(UnderscoreByteSemanticsTest, MultiByteUTF8Characters) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // UTF-8 encoding sizes:
    // - ASCII (U+0000-U+007F): 1 byte
    // - Latin Extended (U+0080-U+07FF): 2 bytes (e.g., é = \xC3\xA9)
    // - CJK (U+4E00-U+9FFF): 3 bytes (e.g., 你 = \xE4\xBD\xA0)
    // - Emoji (U+1F000+): 4 bytes (e.g., 😀 = \xF0\x9F\x98\x80)
    //
    // NOTE: The actual behavior (byte vs codepoint) depends on the regex engine.
    // We use BoostRegexMatcher as the reference to ensure backward compatibility.

    // Test cases: {pattern, test_string}
    std::vector<std::pair<std::string, std::string>> test_cases = {
        // 2-byte character: é
        {"caf_", "caf\xC3\xA9"},
        {"caf__", "caf\xC3\xA9"},
        // 3-byte character: 你
        {"a_b", "a\xE4\xBD\xA0b"},
        {"a__b", "a\xE4\xBD\xA0b"},
        {"a___b", "a\xE4\xBD\xA0b"},
        // 4-byte character: 😀
        {"a___b", "a\xF0\x9F\x98\x80b"},
        {"a____b", "a\xF0\x9F\x98\x80b"},
    };

    for (const auto& [pattern, test_str] : test_cases) {
        auto regex_pattern = translator(pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        // LikePatternMatcher must match original Boost behavior
        EXPECT_EQ(like_matcher(test_str), boost_matcher(test_str))
            << "LikePatternMatcher must match Boost for pattern: " << pattern;
    }
}

TEST(UnderscoreByteSemanticsTest, MixedByteWidths) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern with % and _ mixing with UTF-8
    // String: "hello你好world" = "hello" (5) + "你好" (6 bytes = 2*3) + "world" (5) = 16 bytes
    std::string test_str =
        "hello\xE4\xBD\xA0\xE5\xA5\xBDworld";  // "hello你好world"

    // Test various patterns - all must match original Boost behavior
    std::vector<std::string> patterns = {
        "hello______world",  // 6 underscores
        "hello_____world",   // 5 underscores
        "hello%world",       // % wildcard
        "hello__world",  // 2 underscores (if codepoint mode, might match 2 chars)
    };

    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        EXPECT_EQ(like_matcher(test_str), boost_matcher(test_str))
            << "LikePatternMatcher must match Boost for pattern: " << pattern;
    }
}

TEST(UnderscoreByteSemanticsTest, CorrectnessComparisonWithBoost) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Verify all implementations match original BoostRegexMatcher behavior
    std::vector<std::pair<std::string, std::string>> test_cases = {
        // Pattern, Test string
        {"caf_", "caf\xC3\xA9"},           // café (2-byte é) - 1 underscore
        {"caf__", "caf\xC3\xA9"},          // café (2-byte é) - 2 underscores
        {"a___b", "a\xE4\xBD\xA0b"},       // a你b (3-byte 你)
        {"a____b", "a\xF0\x9F\x98\x80b"},  // a😀b (4-byte emoji)
        {"___", "\xE4\xBD\xA0"},           // 你 alone (3 bytes)
        {"_", "\xE4\xBD\xA0"},             // 你 alone - 1 underscore
        {"______", "\xE4\xBD\xA0\xE5\xA5\xBD"},  // 你好 (6 bytes)
        {"__",
         "\xE4\xBD\xA0\xE5\xA5\xBD"},  // 你好 - 2 underscores (if codepoint mode)
    };

    for (const auto& [pattern, str] : test_cases) {
        auto regex_pattern = translator(pattern);

        // BoostRegexMatcher is the REFERENCE (original behavior)
        BoostRegexMatcher boost_matcher(regex_pattern);
        bool boost_result = boost_matcher(str);

        // All implementations must match Boost
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_EQ(re2_matcher(str), boost_result)
            << "RE2 must match Boost: pattern=" << pattern;
        EXPECT_EQ(like_matcher(str), boost_result)
            << "LikePatternMatcher must match Boost: pattern=" << pattern;
        EXPECT_EQ(smart_matcher(str), boost_result)
            << "SmartPatternMatcher must match Boost: pattern=" << pattern;
    }
}

// ============== Backward Compatibility Test: Match Original Boost Behavior ==============
// CRITICAL: All new implementations must match the ORIGINAL boost::regex behavior exactly.
// BoostRegexMatcher represents the previous implementation and is the reference.
//
// This test ensures:
// 1. RE2 (new RegexMatcher) matches Boost behavior
// 2. LikePatternMatcher matches Boost behavior
// 3. SmartPatternMatcher matches Boost behavior

TEST(BackwardCompatibilityTest, MatchOriginalBoostBehaviorForUTF8) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test cases covering UTF-8 multibyte characters with underscore
    // BoostRegexMatcher is the REFERENCE (original behavior)
    std::vector<std::pair<std::string, std::string>> test_cases = {
        // 2-byte UTF-8: é (U+00E9) = \xC3\xA9
        {"caf_", "caf\xC3\xA9"},   // café with 1 underscore
        {"caf__", "caf\xC3\xA9"},  // café with 2 underscores

        // 3-byte UTF-8: 你 (U+4F60) = \xE4\xBD\xA0
        {"a_b", "a\xE4\xBD\xA0b"},    // a你b with 1 underscore
        {"a__b", "a\xE4\xBD\xA0b"},   // a你b with 2 underscores
        {"a___b", "a\xE4\xBD\xA0b"},  // a你b with 3 underscores

        // 4-byte UTF-8: 😀 (U+1F600) = \xF0\x9F\x98\x80
        {"a___b", "a\xF0\x9F\x98\x80b"},   // a😀b with 3 underscores
        {"a____b", "a\xF0\x9F\x98\x80b"},  // a😀b with 4 underscores

        // Mixed patterns
        {"hello%_world", "hello\xE4\xBD\xA0\xE5\xA5\xBD_world"},
        {"%\xE4\xBD\xA0%", "test\xE4\xBD\xA0test"},
    };

    for (const auto& [like_pattern, test_str] : test_cases) {
        auto regex_pattern = translator(like_pattern);

        // BoostRegexMatcher is the REFERENCE (original implementation)
        BoostRegexMatcher boost_matcher(regex_pattern);
        bool boost_result = boost_matcher(test_str);

        // All new implementations must match Boost
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(like_pattern);
        SmartPatternMatcher smart_matcher(like_pattern);

        EXPECT_EQ(re2_matcher(test_str), boost_result)
            << "RE2 does not match original Boost behavior!\n"
            << "  LIKE pattern: " << like_pattern << "\n"
            << "  Regex pattern: " << regex_pattern << "\n"
            << "  Test string bytes: " << test_str.size() << "\n"
            << "  Boost (reference): " << boost_result << "\n"
            << "  RE2: " << re2_matcher(test_str);

        EXPECT_EQ(like_matcher(test_str), boost_result)
            << "LikePatternMatcher does not match original Boost behavior!\n"
            << "  LIKE pattern: " << like_pattern << "\n"
            << "  Test string bytes: " << test_str.size() << "\n"
            << "  Boost (reference): " << boost_result << "\n"
            << "  LikePatternMatcher: " << like_matcher(test_str);

        EXPECT_EQ(smart_matcher(test_str), boost_result)
            << "SmartPatternMatcher does not match original Boost behavior!\n"
            << "  LIKE pattern: " << like_pattern << "\n"
            << "  Test string bytes: " << test_str.size() << "\n"
            << "  Boost (reference): " << boost_result << "\n"
            << "  SmartPatternMatcher: " << smart_matcher(test_str);
    }
}

TEST(BackwardCompatibilityTest, MatchOriginalBoostBehaviorComprehensive) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Comprehensive test: all pattern types with various strings
    // BoostRegexMatcher is the REFERENCE
    std::vector<std::string> patterns = {
        // Basic patterns
        "abc",
        "%",
        "_",
        "%%",
        "__",
        // Prefix/suffix/contains
        "abc%",
        "%abc",
        "%abc%",
        // With underscores
        "a_c",
        "a__c",
        "_bc",
        "ab_",
        // Complex
        "a%b%c",
        "%a%b%",
        "a%_b",
        "_%a",
        // Escaped
        "100\\%",
        "a\\_b",
        "\\%\\%",
    };

    std::vector<std::string> test_strings = {
        "",
        "a",
        "ab",
        "abc",
        "abcd",
        "aXc",
        "aXXc",
        "Xbc",
        "abX",
        "aXbYc",
        "100%",
        "a_b",
        // UTF-8 strings
        "caf\xC3\xA9",               // café
        "a\xE4\xBD\xA0b",            // a你b
        "\xE4\xBD\xA0\xE5\xA5\xBD",  // 你好
        "hello\xE4\xBD\xA0world",    // hello你world
    };

    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& str : test_strings) {
            bool boost_result = boost_matcher(str);

            EXPECT_EQ(re2_matcher(str), boost_result)
                << "RE2 mismatch: pattern=" << pattern
                << ", str_bytes=" << str.size();
            EXPECT_EQ(like_matcher(str), boost_result)
                << "LikePatternMatcher mismatch: pattern=" << pattern
                << ", str_bytes=" << str.size();
            EXPECT_EQ(smart_matcher(str), boost_result)
                << "SmartPatternMatcher mismatch: pattern=" << pattern
                << ", str_bytes=" << str.size();
        }
    }
}

TEST(LikePatternMatcherTest, SpecialAsciiCharacters) {
    using namespace milvus;

    // Tab and newline
    {
        LikePatternMatcher matcher("%\t%");
        EXPECT_TRUE(matcher(std::string("a\tb")));
        EXPECT_FALSE(matcher(std::string("ab")));
    }

    {
        LikePatternMatcher matcher("%\n%");
        EXPECT_TRUE(matcher(std::string("line1\nline2")));
        EXPECT_FALSE(matcher(std::string("no newline")));
    }

    // Null character in string (edge case)
    {
        LikePatternMatcher matcher("a%b");
        std::string with_null = "a";
        with_null += '\0';
        with_null += "b";
        EXPECT_TRUE(matcher(with_null));
    }
}

// ============== SmartPatternMatcher Edge Cases ==============

TEST(SmartPatternMatcherTest, NonStringTypes) {
    using namespace milvus;

    SmartPatternMatcher matcher("test%");

    EXPECT_FALSE(matcher(123));
    EXPECT_FALSE(matcher(3.14));
    EXPECT_FALSE(matcher(true));
}

TEST(SmartPatternMatcherTest, EmptyPatternAndString) {
    using namespace milvus;

    SmartPatternMatcher empty_pattern("");
    EXPECT_TRUE(empty_pattern(std::string("")));
    EXPECT_FALSE(empty_pattern(std::string("a")));

    SmartPatternMatcher percent("%");
    EXPECT_TRUE(percent(std::string("")));
    EXPECT_TRUE(percent(std::string("anything")));
}

TEST(SmartPatternMatcherTest, StringViewSupportExtended) {
    using namespace milvus;

    SmartPatternMatcher matcher("hello%world");
    std::string_view sv1 = "helloworld";
    std::string_view sv2 = "hello beautiful world";
    std::string_view sv3 = "hello";

    EXPECT_TRUE(matcher(sv1));
    EXPECT_TRUE(matcher(sv2));
    EXPECT_FALSE(matcher(sv3));
}

// ============== Comprehensive Correctness Tests ==============

TEST(CorrectnessTest, AllPatternTypesAgainstRegex) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Exhaustive pattern types
    std::vector<std::string> patterns = {
        // Empty and simple
        "",
        "a",
        "abc",
        // Wildcards only
        "%",
        "%%",
        "_",
        "__",
        "___",
        // Prefix patterns
        "abc%",
        "a%",
        // Suffix patterns
        "%abc",
        "%a",
        // Contains patterns
        "%abc%",
        "%a%",
        // Complex patterns
        "a%b",
        "a%b%c",
        "a%b%c%d",
        "%a%b%c%",
        // Underscore patterns
        "a_c",
        "_bc",
        "ab_",
        "a__c",
        "_a_",
        // Mixed % and _
        "%_",
        "_%",
        "%_%",
        "a%_",
        "_%a",
        "a%_b",
        "a_%b",
        "_a%b_",
        // Escaped characters
        "\\%",
        "\\_",
        "\\\\",
        "100\\%",
        "a\\%b",
        "\\%\\%",
        "a\\_b",
        "\\%%",
        "%\\%",
        // Note: Trailing backslash patterns (abc\, \, %\) are now parse errors
        // and are tested separately in TrailingBackslashIsError
    };

    std::vector<std::string> test_strings = {
        "",    "a",    "ab",  "abc", "abcd",  "xabc",    "abcx", "xabcx",
        "aXc", "aXXc", "Xbc", "abX", "aXbYc", "aXbYcZd", "%",    "%%",
        "_",   "100%", "a%b", "a_b", "\\",    "abc\\",   "a\\b",
    };

    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& str : test_strings) {
            bool regex_result = regex_matcher(str);
            bool smart_result = smart_matcher(str);
            EXPECT_EQ(smart_result, regex_result)
                << "Pattern: \"" << pattern << "\", String: \"" << str << "\""
                << ", Regex: " << regex_result << ", Smart: " << smart_result;
        }
    }
}

TEST(CorrectnessTest, BoundaryConditions) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test patterns at exact boundary lengths
    std::vector<
        std::pair<std::string, std::vector<std::pair<std::string, bool>>>>
        boundary_tests = {
            // Pattern "abc" requires exactly 3 chars
            {"abc", {{"ab", false}, {"abc", true}, {"abcd", false}}},
            // Pattern "a_c" requires exactly 3 chars
            {"a_c", {{"ac", false}, {"abc", true}, {"abXc", false}}},
            // Pattern "a%c" requires at least 2 chars
            {"a%c", {{"ac", true}, {"abc", true}, {"a", false}, {"c", false}}},
            // Pattern "__" requires exactly 2 chars
            {"__", {{"a", false}, {"ab", true}, {"abc", false}}},
            // Pattern "_%_" requires at least 2 chars
            {"_%_", {{"a", false}, {"ab", true}, {"abc", true}}},
        };

    for (const auto& [pattern, cases] : boundary_tests) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& [str, expected] : cases) {
            EXPECT_EQ(smart_matcher(str), expected)
                << "Pattern: " << pattern << ", String: " << str;
            EXPECT_EQ(regex_matcher(str), expected)
                << "Pattern: " << pattern << ", String: " << str
                << " (regex check)";
        }
    }
}

TEST(CorrectnessTest, OverlappingPatternSegments) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test cases where pattern segments could potentially overlap
    std::vector<
        std::pair<std::string, std::vector<std::pair<std::string, bool>>>>
        overlap_tests = {
            // "a%a" - 'a' appears in both prefix and suffix
            {"a%a",
             {{"aa", true},
              {"aba", true},
              {"aXXXa", true},
              {"a", false},
              {"ab", false}}},
            // "ab%ab" - "ab" appears in both
            {"ab%ab",
             {{"abab", true}, {"abXab", true}, {"ab", false}, {"abX", false}}},
            // "%aa%" - searching for "aa"
            {"%aa%",
             {{"aa", true},
              {"baab", true},
              {"aaa", true},
              {"a", false},
              {"ab", false}}},
        };

    for (const auto& [pattern, cases] : overlap_tests) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        SmartPatternMatcher smart_matcher(pattern);

        for (const auto& [str, expected] : cases) {
            EXPECT_EQ(smart_matcher(str), expected)
                << "Pattern: " << pattern << ", String: " << str;
            EXPECT_EQ(regex_matcher(str), expected)
                << "Pattern: " << pattern << ", String: " << str
                << " (regex check)";
        }
    }
}

// ============== Overlapping Pattern Regression Tests ==============
// These tests verify that patterns with overlapping segments work correctly.
// The % wildcard can match zero characters, allowing consecutive segments
// to overlap in the matched string.

TEST(OverlappingPatternRegressionTest, DoubleRepeatedSegment) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "%aa%aa%" - two "aa" segments that can overlap
    {
        std::string pattern = "%aa%aa%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        // "aaa" contains "aa" at positions 0-1 and 1-2 (overlapping)
        EXPECT_TRUE(regex_matcher(std::string("aaa")));
        EXPECT_TRUE(like_matcher(std::string("aaa")));
        EXPECT_TRUE(smart_matcher(std::string("aaa")));

        // "aa" only contains one "aa", should not match
        EXPECT_FALSE(regex_matcher(std::string("aa")));
        EXPECT_FALSE(like_matcher(std::string("aa")));
        EXPECT_FALSE(smart_matcher(std::string("aa")));

        // "aaaa" contains multiple overlapping "aa"
        EXPECT_TRUE(regex_matcher(std::string("aaaa")));
        EXPECT_TRUE(like_matcher(std::string("aaaa")));
        EXPECT_TRUE(smart_matcher(std::string("aaaa")));

        // "aXaa" - non-overlapping
        EXPECT_TRUE(regex_matcher(std::string("aXaa")));
        EXPECT_TRUE(like_matcher(std::string("aXaa")));
        EXPECT_TRUE(smart_matcher(std::string("aXaa")));
    }
}

TEST(OverlappingPatternRegressionTest, PrefixWithRepeatedSuffix) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "a%aa" - starts with 'a', ends with 'aa'
    {
        std::string pattern = "a%aa";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        // "aaa" - 'a' at 0, 'aa' at 1-2
        EXPECT_TRUE(regex_matcher(std::string("aaa")));
        EXPECT_TRUE(like_matcher(std::string("aaa")));
        EXPECT_TRUE(smart_matcher(std::string("aaa")));

        // "aa" - 'a' at 0, 'aa' at 0-1, but 'aa' must come after 'a', so 'aa' at 0-1
        // This works because % can match zero chars
        EXPECT_TRUE(regex_matcher(std::string("aa")));
        EXPECT_TRUE(like_matcher(std::string("aa")));
        EXPECT_TRUE(smart_matcher(std::string("aa")));

        // "a" - too short
        EXPECT_FALSE(regex_matcher(std::string("a")));
        EXPECT_FALSE(like_matcher(std::string("a")));
        EXPECT_FALSE(smart_matcher(std::string("a")));

        // "aXaa" - non-overlapping
        EXPECT_TRUE(regex_matcher(std::string("aXaa")));
        EXPECT_TRUE(like_matcher(std::string("aXaa")));
        EXPECT_TRUE(smart_matcher(std::string("aXaa")));
    }
}

TEST(OverlappingPatternRegressionTest, OverlappingDifferentSegments) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "%ab%ba%" - "ab" and "ba" can overlap in "aba"
    {
        std::string pattern = "%ab%ba%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        // "aba" - "ab" at 0-1, "ba" at 1-2 (overlapping)
        EXPECT_TRUE(regex_matcher(std::string("aba")));
        EXPECT_TRUE(like_matcher(std::string("aba")));
        EXPECT_TRUE(smart_matcher(std::string("aba")));

        // "abba" - "ab" at 0-1, "ba" at 2-3 (non-overlapping)
        EXPECT_TRUE(regex_matcher(std::string("abba")));
        EXPECT_TRUE(like_matcher(std::string("abba")));
        EXPECT_TRUE(smart_matcher(std::string("abba")));

        // "ab" - missing "ba"
        EXPECT_FALSE(regex_matcher(std::string("ab")));
        EXPECT_FALSE(like_matcher(std::string("ab")));
        EXPECT_FALSE(smart_matcher(std::string("ab")));
    }

    // Pattern "%aa%ab%" - "aa" and "ab" can overlap in "aab"
    {
        std::string pattern = "%aa%ab%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        // "aab" - "aa" at 0-1, "ab" at 1-2 (overlapping at 'a')
        EXPECT_TRUE(regex_matcher(std::string("aab")));
        EXPECT_TRUE(like_matcher(std::string("aab")));
        EXPECT_TRUE(smart_matcher(std::string("aab")));

        // "aaab" - multiple options
        EXPECT_TRUE(regex_matcher(std::string("aaab")));
        EXPECT_TRUE(like_matcher(std::string("aaab")));
        EXPECT_TRUE(smart_matcher(std::string("aaab")));
    }
}

TEST(OverlappingPatternRegressionTest, TripleRepeatedSegment) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "%aa%aa%aa%" - three "aa" segments
    {
        std::string pattern = "%aa%aa%aa%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        // "aaaa" - "aa" at 0-1, 1-2, 2-3 (all overlapping)
        EXPECT_TRUE(regex_matcher(std::string("aaaa")));
        EXPECT_TRUE(like_matcher(std::string("aaaa")));
        EXPECT_TRUE(smart_matcher(std::string("aaaa")));

        // "aaa" - only two "aa" possible, should not match
        EXPECT_FALSE(regex_matcher(std::string("aaa")));
        EXPECT_FALSE(like_matcher(std::string("aaa")));
        EXPECT_FALSE(smart_matcher(std::string("aaa")));

        // "aaaaa" - plenty of room
        EXPECT_TRUE(regex_matcher(std::string("aaaaa")));
        EXPECT_TRUE(like_matcher(std::string("aaaaa")));
        EXPECT_TRUE(smart_matcher(std::string("aaaaa")));
    }
}

TEST(OverlappingPatternRegressionTest, OverlappingWithUnderscores) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "%a_%a_%" - segments with underscores
    {
        std::string pattern = "%a_%a_%";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        // "aXaY" - "a_" at 0-1, "a_" at 2-3
        EXPECT_TRUE(regex_matcher(std::string("aXaY")));
        EXPECT_TRUE(like_matcher(std::string("aXaY")));
        EXPECT_TRUE(smart_matcher(std::string("aXaY")));

        // "aaaa" - "a_" at 0-1, "a_" at 1-2 or 2-3 (overlapping possible)
        EXPECT_TRUE(regex_matcher(std::string("aaaa")));
        EXPECT_TRUE(like_matcher(std::string("aaaa")));
        EXPECT_TRUE(smart_matcher(std::string("aaaa")));

        // "aaa" - "a_" at 0-1, "a_" at 1-2 (overlapping)
        EXPECT_TRUE(regex_matcher(std::string("aaa")));
        EXPECT_TRUE(like_matcher(std::string("aaa")));
        EXPECT_TRUE(smart_matcher(std::string("aaa")));
    }
}

TEST(OverlappingPatternRegressionTest, ComprehensiveOverlapTests) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Comprehensive test cases for overlapping patterns
    std::vector<std::tuple<std::string, std::string, bool>> test_cases = {
        // Pattern, String, Expected result
        {"%aa%aa%", "aaa", true},        // Key regression test case
        {"%aa%aa%", "aa", false},        // Only one "aa"
        {"%aa%aa%", "aaaa", true},       // Multiple overlaps
        {"a%aa", "aaa", true},           // Key regression test case
        {"a%aa", "aa", true},            // Overlapping at start
        {"a%aa", "a", false},            // Too short
        {"%ab%bc%", "abc", true},        // "ab" + "bc" overlap
        {"%abc%bcd%", "abcd", true},     // Longer overlap
        {"%aaa%aaa%", "aaaaa", true},    // Three-char overlap
        {"%aaa%aaa%", "aaaa", false},    // Not enough room
        {"%ab%ab%ab%", "ababab", true},  // Non-overlapping repeats
        {"%ab%ab%ab%", "abab", false},   // Only two "ab"
        // Edge cases with single char segments
        {"%a%a%", "aa", true},     // Two single chars, adjacent
        {"%a%a%", "a", false},     // Only one 'a'
        {"%a%a%a%", "aaa", true},  // Three single chars
        {"%a%a%a%", "aa", false},  // Only two 'a'
    };

    for (const auto& [pattern, str, expected] : test_cases) {
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);
        SmartPatternMatcher smart_matcher(pattern);

        EXPECT_EQ(regex_matcher(str), expected)
            << "Regex failed: pattern=" << pattern << ", str=" << str;
        EXPECT_EQ(like_matcher(str), expected)
            << "LikePatternMatcher failed: pattern=" << pattern
            << ", str=" << str;
        EXPECT_EQ(smart_matcher(str), expected)
            << "SmartPatternMatcher failed: pattern=" << pattern
            << ", str=" << str;
    }
}
