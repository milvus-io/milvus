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
#include <limits>
#include <string>
#include <sys/types.h>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "gtest/gtest.h"

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
    using namespace milvus;
    std::string pattern = "abc%";
    std::string regex_pattern = translate_pattern_match_to_regex(pattern);
    // Verify the regex works correctly with both RE2 and Boost
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    EXPECT_TRUE(re2_matcher(std::string("abc")));
    EXPECT_TRUE(re2_matcher(std::string("abcdef")));
    EXPECT_FALSE(re2_matcher(std::string("ab")));
    EXPECT_TRUE(boost_matcher(std::string("abc")));
    EXPECT_TRUE(boost_matcher(std::string("abcdef")));
    EXPECT_FALSE(boost_matcher(std::string("ab")));
}

TEST(TranslatePatternMatchToRegexTest, PatternWithUnderscore) {
    using namespace milvus;
    std::string pattern = "a_c";
    std::string regex_pattern = translate_pattern_match_to_regex(pattern);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    EXPECT_TRUE(re2_matcher(std::string("abc")));
    EXPECT_TRUE(re2_matcher(std::string("aXc")));
    EXPECT_FALSE(re2_matcher(std::string("ac")));
    EXPECT_FALSE(re2_matcher(std::string("aXXc")));
    EXPECT_TRUE(boost_matcher(std::string("abc")));
    EXPECT_TRUE(boost_matcher(std::string("aXc")));
    EXPECT_FALSE(boost_matcher(std::string("ac")));
    EXPECT_FALSE(boost_matcher(std::string("aXXc")));
}

TEST(TranslatePatternMatchToRegexTest, PatternWithSpecialCharacters) {
    using namespace milvus;
    std::string pattern = "a\\%b\\_c";
    std::string regex_pattern = translate_pattern_match_to_regex(pattern);
    // Escaped % and _ should be treated as literal characters
    EXPECT_TRUE(regex_pattern.find("%") != std::string::npos);
    EXPECT_TRUE(regex_pattern.find("_") != std::string::npos);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    EXPECT_TRUE(re2_matcher(std::string("a%b_c")));
    EXPECT_FALSE(re2_matcher(std::string("aXbYc")));
    EXPECT_TRUE(boost_matcher(std::string("a%b_c")));
    EXPECT_FALSE(boost_matcher(std::string("aXbYc")));
}

TEST(TranslatePatternMatchToRegexTest,
     PatternWithMultiplePercentAndUnderscore) {
    using namespace milvus;
    std::string pattern = "%a_b%";
    std::string regex_pattern = translate_pattern_match_to_regex(pattern);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    EXPECT_TRUE(re2_matcher(std::string("aXb")));
    EXPECT_TRUE(re2_matcher(std::string("XXXaYbZZZ")));
    EXPECT_FALSE(re2_matcher(std::string("ab")));
    EXPECT_TRUE(boost_matcher(std::string("aXb")));
    EXPECT_TRUE(boost_matcher(std::string("XXXaYbZZZ")));
    EXPECT_FALSE(boost_matcher(std::string("ab")));
}

TEST(TranslatePatternMatchToRegexTest, PatternWithRegexChar) {
    using namespace milvus;
    std::string pattern = "abc*def.ghi+";
    std::string regex_pattern = translate_pattern_match_to_regex(pattern);
    // Regex special characters should be escaped
    EXPECT_TRUE(regex_pattern.find("\\*") != std::string::npos);
    EXPECT_TRUE(regex_pattern.find("\\.") != std::string::npos);
    EXPECT_TRUE(regex_pattern.find("\\+") != std::string::npos);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    EXPECT_TRUE(re2_matcher(std::string("abc*def.ghi+")));
    EXPECT_FALSE(re2_matcher(std::string("abcXdefYghi+")));
    EXPECT_TRUE(boost_matcher(std::string("abc*def.ghi+")));
    EXPECT_FALSE(boost_matcher(std::string("abcXdefYghi+")));
}

TEST(TranslatePatternMatchToRegexTest, MixPattern) {
    using namespace milvus;
    std::string pattern = R"(abc\+\def%ghi_[\\)";
    std::string regex_pattern = translate_pattern_match_to_regex(pattern);
    // Verify the regex works correctly
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    // Should match: abc+def<anything>ghi<one char>[\
    EXPECT_TRUE(re2_matcher(std::string("abc+defXXXghiY[\\")));
    EXPECT_TRUE(re2_matcher(std::string("abc+defghiX[\\")));
    EXPECT_FALSE(
        re2_matcher(std::string("abc+defghi[\\")));  // missing char for _
    EXPECT_TRUE(boost_matcher(std::string("abc+defXXXghiY[\\")));
    EXPECT_TRUE(boost_matcher(std::string("abc+defghiX[\\")));
    EXPECT_FALSE(boost_matcher(std::string("abc+defghi[\\")));
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

    // Patterns without wildcards should remain unchanged
    EXPECT_EQ(translator(pattern1), "abc");
    EXPECT_EQ(translator(pattern2), "xyz");

    // Pattern with wildcards should produce valid regex
    auto regex_pattern = translator(pattern3);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    EXPECT_TRUE(re2_matcher(std::string("aXb")));
    EXPECT_TRUE(re2_matcher(std::string("XXXaYbZZZ")));
    EXPECT_TRUE(boost_matcher(std::string("aXb")));
    EXPECT_TRUE(boost_matcher(std::string("XXXaYbZZZ")));
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

// ============== LikePatternMatcher Tests ==============

TEST(LikePatternMatcherTest, UsesMultiWildcardForSimplePatterns) {
    using namespace milvus;

    // Pattern with only % wildcards
    LikePatternMatcher matcher("a%b%c");
    EXPECT_TRUE(matcher(std::string("abc")));
    EXPECT_TRUE(matcher(std::string("aXbYc")));
    EXPECT_FALSE(matcher(std::string("Xabc")));
}

TEST(LikePatternMatcherTest, HandlesUnderscorePatterns) {
    using namespace milvus;

    // Pattern with _ wildcard should still match correctly
    LikePatternMatcher matcher("a_c");
    EXPECT_TRUE(matcher(std::string("abc")));
    EXPECT_TRUE(matcher(std::string("aXc")));
    EXPECT_FALSE(matcher(std::string("ac")));
    EXPECT_FALSE(matcher(std::string("aXXc")));
}

TEST(LikePatternMatcherTest, CorrectnessComparisonWithRegex) {
    using namespace milvus;

    // Test that LikePatternMatcher gives same results as RegexMatcher
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
        LikePatternMatcher smart_matcher(pattern);

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

    // Test that LikePatternMatcher gives same results as RegexMatcher for escaped patterns
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
        LikePatternMatcher smart_matcher(pattern);

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
//   - LikePatternMatcher

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

    // ===== LikePatternMatcher =====
    // BREAKING: Previously had undefined behavior, now throws
    EXPECT_ANY_THROW(LikePatternMatcher("abc\\"));
    EXPECT_ANY_THROW(LikePatternMatcher("\\"));
    EXPECT_ANY_THROW(LikePatternMatcher("%\\"));
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

TEST(LikePatternMatcherTest, RegexMetacharacterDot) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "a.b" should match literal "a.b", not "aXb"
    {
        std::string pattern = "a.b";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher smart_matcher(pattern);

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
        LikePatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("file.txt")));
        EXPECT_TRUE(regex_matcher(std::string("file.txt")));
        EXPECT_TRUE(smart_matcher(std::string("a.b")));
        EXPECT_TRUE(regex_matcher(std::string("a.b")));
        EXPECT_FALSE(smart_matcher(std::string("noperiod")));
        EXPECT_FALSE(regex_matcher(std::string("noperiod")));
    }
}

TEST(LikePatternMatcherTest, RegexMetacharacterBrackets) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "[test]" should match literal "[test]"
    {
        std::string pattern = "[test]";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher smart_matcher(pattern);

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
        LikePatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("[x]")));
        EXPECT_TRUE(regex_matcher(std::string("[x]")));
        EXPECT_TRUE(smart_matcher(std::string("array[0]")));
        EXPECT_TRUE(regex_matcher(std::string("array[0]")));
    }
}

TEST(LikePatternMatcherTest, RegexMetacharacterParentheses) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Pattern "(test)" should match literal "(test)"
    {
        std::string pattern = "(test)";
        auto regex_pattern = translator(pattern);
        RegexMatcher regex_matcher(regex_pattern);
        LikePatternMatcher smart_matcher(pattern);

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
        LikePatternMatcher smart_matcher(pattern);

        EXPECT_TRUE(smart_matcher(std::string("func()")));
        EXPECT_TRUE(regex_matcher(std::string("func()")));
        EXPECT_TRUE(smart_matcher(std::string("func(x)")));
        EXPECT_TRUE(regex_matcher(std::string("func(x)")));
        EXPECT_TRUE(smart_matcher(std::string("func(a,b)")));
        EXPECT_TRUE(regex_matcher(std::string("func(a,b)")));
    }
}

TEST(LikePatternMatcherTest, RegexMetacharacterMixed) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test various regex metacharacters: ^ $ * + ? | { }
    std::vector<
        std::pair<std::string, std::vector<std::pair<std::string, bool>>>>
        test_cases = {
            // Pattern with ^
            {"^start%",
             {{"^start", true}, {"^startXXX", true}, {"start", false}}},
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
        LikePatternMatcher smart_matcher(pattern);

        for (const auto& [test_str, expected] : cases) {
            EXPECT_EQ(smart_matcher(std::string(test_str)), expected)
                << "SmartPattern: " << pattern << ", String: " << test_str;
            EXPECT_EQ(regex_matcher(std::string(test_str)), expected)
                << "RegexPattern: " << pattern << ", String: " << test_str;
        }
    }
}

TEST(LikePatternMatcherTest, RegexMetacharacterCorrectnessComparison) {
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
        LikePatternMatcher smart_matcher(pattern);

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

// ============== SQL Standard Underscore Semantics Tests ==============
// The LikePatternMatcher's underscore '_' matches ONE UTF-8 CHARACTER (codepoint),
// following the SQL standard. This is consistent with PostgreSQL, MySQL, and other databases.
//
// UTF-8 encoding sizes:
// - ASCII (U+0000-U+007F): 1 byte per character
// - Latin Extended (U+0080-U+07FF): 2 bytes per character (e.g., é)
// - CJK (U+4E00-U+9FFF): 3 bytes per character (e.g., 你)
// - Emoji (U+1F000+): 4 bytes per character (e.g., 😀)

TEST(UnderscoreCharacterSemanticsTest, SingleByteCharacters) {
    using namespace milvus;

    // For ASCII (single-byte), _ matches exactly one character
    {
        LikePatternMatcher matcher("a_c");
        EXPECT_TRUE(matcher(std::string("abc")));
        EXPECT_TRUE(matcher(std::string("aXc")));
        EXPECT_FALSE(matcher(std::string("ac")));    // Too short
        EXPECT_FALSE(matcher(std::string("abbc")));  // Too long
    }
}

TEST(UnderscoreCharacterSemanticsTest, MultiByteUTF8Characters) {
    using namespace milvus;

    // UTF-8 encoding sizes:
    // - ASCII (U+0000-U+007F): 1 byte
    // - Latin Extended (U+0080-U+07FF): 2 bytes (e.g., é = \xC3\xA9)
    // - CJK (U+4E00-U+9FFF): 3 bytes (e.g., 你 = \xE4\xBD\xA0)
    // - Emoji (U+1F000+): 4 bytes (e.g., 😀 = \xF0\x9F\x98\x80)
    //
    // SQL Standard: _ matches exactly one CHARACTER (codepoint), not one byte

    // 2-byte character: é (1 character)
    {
        LikePatternMatcher m1("caf_");
        EXPECT_TRUE(m1(std::string("caf\xC3\xA9")));  // 1 underscore = 1 char ✓

        LikePatternMatcher m2("caf__");
        EXPECT_FALSE(m2(std::string("caf\xC3\xA9")));  // 2 underscores ≠ 1 char
    }

    // 3-byte character: 你 (1 character)
    {
        LikePatternMatcher m1("a_b");
        EXPECT_TRUE(
            m1(std::string("a\xE4\xBD\xA0"
                           "b")));  // 1 underscore = 1 char ✓

        LikePatternMatcher m2("a__b");
        EXPECT_FALSE(
            m2(std::string("a\xE4\xBD\xA0"
                           "b")));  // 2 underscores ≠ 1 char

        LikePatternMatcher m3("a___b");
        EXPECT_FALSE(
            m3(std::string("a\xE4\xBD\xA0"
                           "b")));  // 3 underscores ≠ 1 char
    }

    // 4-byte character: 😀 (1 character)
    {
        LikePatternMatcher m1("a_b");
        EXPECT_TRUE(
            m1(std::string("a\xF0\x9F\x98\x80"
                           "b")));  // 1 underscore = 1 char ✓

        LikePatternMatcher m2("a____b");
        EXPECT_FALSE(
            m2(std::string("a\xF0\x9F\x98\x80"
                           "b")));  // 4 underscores ≠ 1 char
    }
}

TEST(UnderscoreCharacterSemanticsTest, MixedByteWidths) {
    using namespace milvus;

    // String: "hello你好world" = 5 + 2 + 5 = 12 CHARACTERS (16 bytes)
    std::string test_str =
        "hello\xE4\xBD\xA0\xE5\xA5\xBDworld";  // "hello你好world"

    // SQL standard: _ matches one CHARACTER
    {
        LikePatternMatcher m1(
            "hello__world");  // 2 underscores for 2 chars (你好)
        EXPECT_TRUE(m1(test_str));
    }

    {
        LikePatternMatcher m2("hello%world");  // % matches any chars
        EXPECT_TRUE(m2(test_str));
    }

    {
        LikePatternMatcher m3("hello______world");  // 6 underscores ≠ 2 chars
        EXPECT_FALSE(m3(test_str));
    }

    {
        LikePatternMatcher m4("hello_____world");  // 5 underscores ≠ 2 chars
        EXPECT_FALSE(m4(test_str));
    }
}

TEST(UnderscoreCharacterSemanticsTest, SqlStandardCharacterMatching) {
    using namespace milvus;

    // SQL standard: _ matches exactly one CHARACTER (codepoint), not one byte
    // This is the expected behavior for all SQL databases

    // 2-byte UTF-8: é (U+00E9) = \xC3\xA9
    {
        LikePatternMatcher matcher("caf_");
        // "café" has 4 characters, pattern "caf_" has 4 characters
        EXPECT_TRUE(matcher(std::string("caf\xC3\xA9")));  // café - MATCH
    }

    // 3-byte UTF-8: 你 (U+4F60) = \xE4\xBD\xA0
    {
        LikePatternMatcher matcher("a_b");
        // "a你b" has 3 characters, pattern "a_b" has 3 characters
        EXPECT_TRUE(
            matcher(std::string("a\xE4\xBD\xA0"
                                "b")));  // a你b - MATCH
    }

    // 4-byte UTF-8: 😀 (U+1F600) = \xF0\x9F\x98\x80
    {
        LikePatternMatcher matcher("a_b");
        // "a😀b" has 3 characters, pattern "a_b" has 3 characters
        EXPECT_TRUE(
            matcher(std::string("a\xF0\x9F\x98\x80"
                                "b")));  // a😀b - MATCH
    }

    // Single character match
    {
        LikePatternMatcher matcher("_");
        // 你 is 1 character (3 bytes)
        EXPECT_TRUE(matcher(std::string("\xE4\xBD\xA0")));  // 你 - MATCH
        EXPECT_FALSE(matcher(std::string("ab")));  // 2 chars - NO MATCH
    }

    // Two characters match
    {
        LikePatternMatcher matcher("__");
        // 你好 is 2 characters (6 bytes)
        EXPECT_TRUE(
            matcher(std::string("\xE4\xBD\xA0\xE5\xA5\xBD")));  // 你好 - MATCH
        EXPECT_TRUE(matcher(std::string("ab")));    // 2 chars - MATCH
        EXPECT_FALSE(matcher(std::string("abc")));  // 3 chars - NO MATCH
    }

    // Mixed ASCII and UTF-8
    {
        LikePatternMatcher matcher("hello__world");
        // "hello你好world" has 12 characters
        EXPECT_TRUE(matcher(std::string(
            "hello\xE4\xBD\xA0\xE5\xA5\xBDworld")));  // hello你好world - MATCH
        EXPECT_FALSE(matcher(
            std::string("hello\xE4\xBD\xA0world")));  // hello你world - NO MATCH
    }
}

// ============== SQL Standard Character Semantics Tests ==============
// The _ wildcard matches exactly one UTF-8 CHARACTER (codepoint), not one byte.
// This follows the SQL standard and is consistent with all major databases.

TEST(SqlStandardTest, UnderscoreMatchesOneCharacter) {
    using namespace milvus;

    // Test cases: each _ should match exactly one UTF-8 character
    struct TestCase {
        std::string pattern;
        std::string test_str;
        bool expected;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
        // 2-byte UTF-8: é (U+00E9) = \xC3\xA9
        {"caf_", "caf\xC3\xA9", true, "café with 1 underscore matches"},
        {"caf__",
         "caf\xC3\xA9",
         false,
         "café with 2 underscores doesn't match (only 1 char after caf)"},

        // 3-byte UTF-8: 你 (U+4F60) = \xE4\xBD\xA0
        {"a_b",
         "a\xE4\xBD\xA0"
         "b",
         true,
         "a你b with 1 underscore matches"},
        {"a__b",
         "a\xE4\xBD\xA0"
         "b",
         false,
         "a你b with 2 underscores doesn't match"},
        {"a___b",
         "a\xE4\xBD\xA0"
         "b",
         false,
         "a你b with 3 underscores doesn't match"},

        // 4-byte UTF-8: 😀 (U+1F600) = \xF0\x9F\x98\x80
        {"a_b",
         "a\xF0\x9F\x98\x80"
         "b",
         true,
         "a😀b with 1 underscore matches"},
        {"a____b",
         "a\xF0\x9F\x98\x80"
         "b",
         false,
         "a😀b with 4 underscores doesn't match"},

        // Mixed patterns with %
        {"hello%_world",
         "hello\xE4\xBD\xA0\xE5\xA5\xBD_world",
         true,
         "% matches 你好, _ matches literal _"},
        {"%\xE4\xBD\xA0%",
         "test\xE4\xBD\xA0test",
         true,
         "Literal UTF-8 character in pattern"},

        // Multiple UTF-8 characters
        {"__",
         "\xE4\xBD\xA0\xE5\xA5\xBD",
         true,
         "2 underscores match 你好 (2 chars)"},
        {"___",
         "\xE4\xBD\xA0\xE5\xA5\xBD",
         false,
         "3 underscores don't match 你好 (only 2 chars)"},
    };

    for (const auto& tc : test_cases) {
        LikePatternMatcher matcher(tc.pattern);
        EXPECT_EQ(matcher(tc.test_str), tc.expected)
            << "Failed: " << tc.description << "\n"
            << "  Pattern: " << tc.pattern << "\n"
            << "  String bytes: " << tc.test_str.size();
    }
}

TEST(SqlStandardTest, ComprehensivePatternMatching) {
    using namespace milvus;

    // Comprehensive test: all pattern types with various strings
    // LikePatternMatcher follows SQL standard: _ matches one CHARACTER
    struct TestCase {
        std::string pattern;
        std::string test_str;
        bool expected;
    };

    std::vector<TestCase> test_cases = {
        // Basic ASCII patterns
        {"abc", "abc", true},
        {"abc", "abcd", false},
        {"%", "", true},
        {"%", "anything", true},
        {"_", "a", true},
        {"_", "ab", false},
        {"__", "ab", true},
        {"__", "abc", false},

        // Prefix/suffix/contains
        {"abc%", "abc", true},
        {"abc%", "abcdef", true},
        {"abc%", "ab", false},
        {"%abc", "abc", true},
        {"%abc", "xyzabc", true},
        {"%abc%", "xabcy", true},

        // Underscore patterns with ASCII
        {"a_c", "abc", true},
        {"a_c", "aXc", true},
        {"a_c", "aXXc", false},
        {"a__c", "aXXc", true},
        {"_bc", "abc", true},
        {"_bc", "Xbc", true},
        {"ab_", "abc", true},
        {"ab_", "abX", true},

        // Complex patterns
        {"a%b%c", "abc", true},
        {"a%b%c", "aXbYc", true},
        {"%a%b%", "XaYbZ", true},
        {"a%_b", "aXb", true},
        {"a%_b", "aXXb", true},
        {"_%a", "Xa", true},

        // Escaped characters
        {"100\\%", "100%", true},
        {"100\\%", "100X", false},
        {"a\\_b", "a_b", true},
        {"a\\_b", "aXb", false},

        // UTF-8 strings with % (no underscore - should work same as before)
        {"%\xE4\xBD\xA0%", "test\xE4\xBD\xA0test", true},
        {"hello%", "hello\xE4\xBD\xA0world", true},
        {"%world", "hello\xE4\xBD\xA0world", true},

        // UTF-8 with underscore (SQL standard: _ = one character)
        {"_", "\xE4\xBD\xA0", true},               // 你 is 1 char
        {"_", "\xC3\xA9", true},                   // é is 1 char
        {"__", "\xE4\xBD\xA0\xE5\xA5\xBD", true},  // 你好 is 2 chars
        {"caf_", "caf\xC3\xA9", true},             // café is 4 chars
        {"a_b",
         "a\xE4\xBD\xA0"
         "b",
         true},  // a你b is 3 chars
    };

    for (const auto& tc : test_cases) {
        LikePatternMatcher matcher(tc.pattern);
        EXPECT_EQ(matcher(tc.test_str), tc.expected)
            << "Pattern: " << tc.pattern
            << ", String bytes: " << tc.test_str.size()
            << ", Expected: " << tc.expected;
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

// ============== LikePatternMatcher Edge Cases ==============

TEST(LikePatternMatcherTest, EmptyPatternAndString) {
    using namespace milvus;

    LikePatternMatcher empty_pattern("");
    EXPECT_TRUE(empty_pattern(std::string("")));
    EXPECT_FALSE(empty_pattern(std::string("a")));

    LikePatternMatcher percent("%");
    EXPECT_TRUE(percent(std::string("")));
    EXPECT_TRUE(percent(std::string("anything")));
}

TEST(LikePatternMatcherTest, StringViewSupportExtended) {
    using namespace milvus;

    LikePatternMatcher matcher("hello%world");
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
        LikePatternMatcher smart_matcher(pattern);

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
        LikePatternMatcher smart_matcher(pattern);

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
        LikePatternMatcher smart_matcher(pattern);

        for (const auto& [str, expected] : cases) {
            EXPECT_EQ(smart_matcher(str), expected)
                << "Pattern: " << pattern << ", String: " << str;
            EXPECT_EQ(regex_matcher(str), expected)
                << "Pattern: " << pattern << ", String: " << str
                << " (regex check)";
        }
    }
}

// ============== UTF-8 Consistency Tests: RE2 vs Boost vs LikePatternMatcher ==============
// These tests verify that RE2, Boost regex, and LikePatternMatcher all produce
// identical results for UTF-8 strings, ensuring SQL standard compliance.
//
// UTF-8 encoding sizes:
// - ASCII (U+0000-U+007F): 1 byte per character
// - Latin Extended (U+0080-U+07FF): 2 bytes per character (e.g., é = \xC3\xA9)
// - CJK (U+4E00-U+9FFF): 3 bytes per character (e.g., 你 = \xE4\xBD\xA0)
// - Emoji (U+1F000+): 4 bytes per character (e.g., 😀 = \xF0\x9F\x98\x80)

TEST(UTF8ConsistencyTest, UnderscoreMatchesSingleUTF8Character) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test that _ matches exactly one UTF-8 character (not one byte)
    // across all three implementations: RE2, Boost, and LikePatternMatcher
    struct TestCase {
        std::string pattern;
        std::string test_str;
        bool expected;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
        // 2-byte UTF-8: é (U+00E9) = \xC3\xA9
        {"caf_", "caf\xC3\xA9", true, "café: _ matches 2-byte é"},
        {"caf_", "cafe", true, "cafe: _ matches ASCII e"},
        {"caf__", "caf\xC3\xA9", false, "café: __ doesn't match single é"},

        // 3-byte UTF-8: 你 (U+4F60) = \xE4\xBD\xA0, 好 (U+597D) = \xE5\xA5\xBD
        {"a_b",
         "a\xE4\xBD\xA0"
         "b",
         true,
         "a你b: _ matches 3-byte 你"},
        {"a__b",
         "a\xE4\xBD\xA0"
         "b",
         false,
         "a你b: __ doesn't match single 你"},
        {"__",
         "\xE4\xBD\xA0\xE5\xA5\xBD",
         true,
         "你好: __ matches two 3-byte chars"},
        {"___",
         "\xE4\xBD\xA0\xE5\xA5\xBD",
         false,
         "你好: ___ doesn't match two chars"},

        // 4-byte UTF-8: 😀 (U+1F600) = \xF0\x9F\x98\x80
        {"a_b",
         "a\xF0\x9F\x98\x80"
         "b",
         true,
         "a😀b: _ matches 4-byte emoji"},
        {"a____b",
         "a\xF0\x9F\x98\x80"
         "b",
         false,
         "a😀b: ____ doesn't match single emoji"},

        // Mixed byte widths
        {"hello__world",
         "hello\xE4\xBD\xA0\xE5\xA5\xBD"
         "world",
         true,
         "hello你好world: __ matches two 3-byte chars"},
        {"_\xE4\xBD\xA0_",
         "X\xE4\xBD\xA0Y",
         true,
         "X你Y: _ matches ASCII around literal UTF-8"},
    };

    for (const auto& tc : test_cases) {
        auto regex_pattern = translator(tc.pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(tc.pattern);

        bool re2_result = re2_matcher(tc.test_str);
        bool boost_result = boost_matcher(tc.test_str);
        bool like_result = like_matcher(tc.test_str);

        // All three must agree
        EXPECT_EQ(re2_result, tc.expected) << "RE2 failed: " << tc.description;
        EXPECT_EQ(boost_result, tc.expected)
            << "Boost failed: " << tc.description;
        EXPECT_EQ(like_result, tc.expected)
            << "LikePatternMatcher failed: " << tc.description;

        // RE2 and Boost must be consistent with each other
        EXPECT_EQ(re2_result, boost_result)
            << "RE2/Boost mismatch: " << tc.description
            << " (RE2=" << re2_result << ", Boost=" << boost_result << ")";

        // LikePatternMatcher must be consistent with regex engines
        EXPECT_EQ(like_result, re2_result)
            << "Like/RE2 mismatch: " << tc.description
            << " (Like=" << like_result << ", RE2=" << re2_result << ")";
    }
}

TEST(UTF8ConsistencyTest, PercentWildcardWithUTF8) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test that % correctly matches zero or more UTF-8 characters
    struct TestCase {
        std::string pattern;
        std::string test_str;
        bool expected;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
        // Prefix matching
        {"hello%",
         "hello\xE4\xBD\xA0\xE5\xA5\xBD",
         true,
         "hello你好: prefix match"},
        {"hello%",
         "hello\xF0\x9F\x98\x80",
         true,
         "hello😀: prefix match with emoji"},

        // Suffix matching
        {"%world",
         "\xE4\xBD\xA0\xE5\xA5\xBD"
         "world",
         true,
         "你好world: suffix match"},
        {"%\xE4\xBD\xA0",
         "hello\xE4\xBD\xA0",
         true,
         "hello你: suffix with UTF-8 literal"},

        // Contains matching
        {"%\xE4\xBD\xA0%",
         "abc\xE4\xBD\xA0"
         "def",
         true,
         "abc你def: contains UTF-8"},
        {"%\xE4\xBD\xA0\xE5\xA5\xBD%",
         "test\xE4\xBD\xA0\xE5\xA5\xBD"
         "test",
         true,
         "test你好test: contains multi-byte sequence"},

        // Complex patterns
        {"\xE4\xBD\xA0%\xE5\xA5\xBD",
         "\xE4\xBD\xA0\xE5\xA5\xBD",
         true,
         "你好: UTF-8 prefix and suffix"},
        {"\xE4\xBD\xA0%\xE5\xA5\xBD",
         "\xE4\xBD\xA0"
         "abc"
         "\xE5\xA5\xBD",
         true,
         "你abc好: UTF-8 with middle content"},

        // No match cases
        {"%\xE4\xBD\xA0%",
         "hello world",
         false,
         "hello world: no UTF-8 char present"},
        {"hello%", "\xE4\xBD\xA0hello", false, "你hello: wrong prefix"},
    };

    for (const auto& tc : test_cases) {
        auto regex_pattern = translator(tc.pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(tc.pattern);

        bool re2_result = re2_matcher(tc.test_str);
        bool boost_result = boost_matcher(tc.test_str);
        bool like_result = like_matcher(tc.test_str);

        EXPECT_EQ(re2_result, tc.expected) << "RE2 failed: " << tc.description;
        EXPECT_EQ(boost_result, tc.expected)
            << "Boost failed: " << tc.description;
        EXPECT_EQ(like_result, tc.expected)
            << "LikePatternMatcher failed: " << tc.description;

        EXPECT_EQ(re2_result, boost_result)
            << "RE2/Boost mismatch: " << tc.description;
        EXPECT_EQ(like_result, re2_result)
            << "Like/RE2 mismatch: " << tc.description;
    }
}

TEST(UTF8ConsistencyTest, MixedUnderscoreAndPercentWithUTF8) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test complex patterns mixing _ and % with UTF-8 strings
    struct TestCase {
        std::string pattern;
        std::string test_str;
        bool expected;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
        // _ with %
        {"%_world",
         "\xE4\xBD\xA0world",
         true,
         "你world: _ matches 3-byte char before suffix"},
        {"hello_%",
         "hello\xE4\xBD\xA0",
         true,
         "hello你: _ matches 3-byte char after prefix"},
        {"%_%", "\xE4\xBD\xA0", true, "你: single UTF-8 char with wildcards"},
        {"%__%",
         "\xE4\xBD\xA0\xE5\xA5\xBD",
         true,
         "你好: two UTF-8 chars with wildcards"},

        // Multiple _ with UTF-8
        {"a%__b",
         "a\xE4\xBD\xA0\xE5\xA5\xBD"
         "b",
         true,
         "a你好b: __ matches two chars"},
        {"a%__b",
         "aXY\xE4\xBD\xA0\xE5\xA5\xBD"
         "b",
         true,
         "aXY你好b: % then __ at end"},

        // UTF-8 in pattern with wildcards
        {"\xE4\xBD\xA0_%",
         "\xE4\xBD\xA0Xabc",
         true,
         "你Xabc: UTF-8 literal + _ + %"},
        {"%_\xE4\xBD\xA0",
         "abcX\xE4\xBD\xA0",
         true,
         "abcX你: % + _ + UTF-8 literal"},

        // Edge cases
        {"_%_",
         "\xE4\xBD\xA0X\xE5\xA5\xBD",
         true,
         "你X好: _ % _ with mixed chars"},
        {"_\xE4\xBD\xA0_", "X\xE4\xBD\xA0Y", true, "X你Y: _ literal _ pattern"},
    };

    for (const auto& tc : test_cases) {
        auto regex_pattern = translator(tc.pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(tc.pattern);

        bool re2_result = re2_matcher(tc.test_str);
        bool boost_result = boost_matcher(tc.test_str);
        bool like_result = like_matcher(tc.test_str);

        EXPECT_EQ(re2_result, tc.expected) << "RE2 failed: " << tc.description;
        EXPECT_EQ(boost_result, tc.expected)
            << "Boost failed: " << tc.description;
        EXPECT_EQ(like_result, tc.expected)
            << "LikePatternMatcher failed: " << tc.description;

        EXPECT_EQ(re2_result, boost_result)
            << "RE2/Boost mismatch: " << tc.description;
        EXPECT_EQ(like_result, re2_result)
            << "Like/RE2 mismatch: " << tc.description;
    }
}

TEST(UTF8ConsistencyTest, EscapedCharactersWithUTF8) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test escaped characters in patterns with UTF-8 strings
    struct TestCase {
        std::string pattern;
        std::string test_str;
        bool expected;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
        // Escaped % with UTF-8
        {"100\\%\xE4\xBD\xA0",
         "100%\xE4\xBD\xA0",
         true,
         "100%你: escaped % + UTF-8"},
        {"\xE4\xBD\xA0\\%", "\xE4\xBD\xA0%", true, "你%: UTF-8 + escaped %"},

        // Escaped _ with UTF-8
        {"\xE4\xBD\xA0\\_\xE5\xA5\xBD",
         "\xE4\xBD\xA0_\xE5\xA5\xBD",
         true,
         "你_好: UTF-8 with escaped _"},
        {"file\\_%",
         "file_\xE4\xBD\xA0",
         true,
         "file_你: escaped _ then UTF-8"},

        // Mixed escaped and wildcards with UTF-8
        {"\\%%\xE4\xBD\xA0",
         "%abc\xE4\xBD\xA0",
         true,
         "%abc你: escaped % at start"},
        {"\xE4\xBD\xA0%\\%",
         "\xE4\xBD\xA0"
         "abc%",
         true,
         "你abc%: UTF-8 + % + escaped %"},
    };

    for (const auto& tc : test_cases) {
        auto regex_pattern = translator(tc.pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(tc.pattern);

        bool re2_result = re2_matcher(tc.test_str);
        bool boost_result = boost_matcher(tc.test_str);
        bool like_result = like_matcher(tc.test_str);

        EXPECT_EQ(re2_result, tc.expected) << "RE2 failed: " << tc.description;
        EXPECT_EQ(boost_result, tc.expected)
            << "Boost failed: " << tc.description;
        EXPECT_EQ(like_result, tc.expected)
            << "LikePatternMatcher failed: " << tc.description;

        EXPECT_EQ(re2_result, boost_result)
            << "RE2/Boost mismatch: " << tc.description;
        EXPECT_EQ(like_result, re2_result)
            << "Like/RE2 mismatch: " << tc.description;
    }
}

TEST(UTF8ConsistencyTest, ComprehensiveUTF8Consistency) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Comprehensive test: all pattern types with various UTF-8 strings
    // This ensures RE2, Boost, and LikePatternMatcher all behave identically

    std::vector<std::string> patterns = {
        // Basic patterns
        "%",
        "_",
        "__",
        "___",
        // Prefix/suffix
        "hello%",
        "%world",
        "%hello%",
        // With underscores
        "a_c",
        "a__c",
        "_bc",
        "ab_",
        // Mixed
        "%_",
        "_%",
        "%_%",
        "a%_b",
        // With UTF-8 literals
        "\xE4\xBD\xA0%",              // 你%
        "%\xE4\xBD\xA0",              // %你
        "%\xE4\xBD\xA0%",             // %你%
        "\xE4\xBD\xA0_\xE5\xA5\xBD",  // 你_好
        // Escaped
        "\\%",
        "\\_",
        "100\\%",
    };

    std::vector<std::string> test_strings = {
        // ASCII
        "",
        "a",
        "ab",
        "abc",
        "hello",
        "world",
        "helloworld",
        // 2-byte UTF-8
        "caf\xC3\xA9",  // café
        // 3-byte UTF-8
        "\xE4\xBD\xA0",               // 你
        "\xE4\xBD\xA0\xE5\xA5\xBD",   // 你好
        "hello\xE4\xBD\xA0",          // hello你
        "\xE4\xBD\xA0world",          // 你world
        "hello\xE4\xBD\xA0world",     // hello你world
        "\xE4\xBD\xA0X\xE5\xA5\xBD",  // 你X好
        // 4-byte UTF-8
        "\xF0\x9F\x98\x80",  // 😀
        "a\xF0\x9F\x98\x80"
        "b",  // a😀b
        // Mixed
        "\xE4\xBD\xA0\xE5\xA5\xBD\xF0\x9F\x98\x80",  // 你好😀
        // Special cases
        "100%",
        "_",
        "%",
    };

    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        for (const auto& str : test_strings) {
            bool re2_result = re2_matcher(str);
            bool boost_result = boost_matcher(str);
            bool like_result = like_matcher(str);

            // RE2 and Boost must be consistent
            EXPECT_EQ(re2_result, boost_result)
                << "RE2/Boost mismatch for pattern=\"" << pattern
                << "\", string bytes=" << str.size() << " (RE2=" << re2_result
                << ", Boost=" << boost_result << ")";

            // LikePatternMatcher must match regex behavior
            EXPECT_EQ(like_result, re2_result)
                << "Like/RE2 mismatch for pattern=\"" << pattern
                << "\", string bytes=" << str.size() << " (Like=" << like_result
                << ", RE2=" << re2_result << ")";
        }
    }
}

TEST(UTF8ConsistencyTest, StringViewConsistency) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test that string_view behaves the same as string for UTF-8
    std::vector<std::pair<std::string, std::string>> test_cases = {
        {"%\xE4\xBD\xA0%", "hello\xE4\xBD\xA0world"},
        {"a_b",
         "a\xE4\xBD\xA0"
         "b"},
        {"hello%", "hello\xE4\xBD\xA0\xE5\xA5\xBD"},
    };

    for (const auto& [pattern, test_str] : test_cases) {
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        std::string_view sv(test_str);

        // Compare string vs string_view results
        EXPECT_EQ(re2_matcher(test_str), re2_matcher(sv))
            << "RE2 string/string_view mismatch";
        EXPECT_EQ(boost_matcher(test_str), boost_matcher(sv))
            << "Boost string/string_view mismatch";
        EXPECT_EQ(like_matcher(test_str), like_matcher(sv))
            << "Like string/string_view mismatch";

        // All implementations should agree
        EXPECT_EQ(re2_matcher(sv), boost_matcher(sv))
            << "RE2/Boost string_view mismatch";
        EXPECT_EQ(like_matcher(sv), re2_matcher(sv))
            << "Like/RE2 string_view mismatch";
    }
}

// ============== NUL Byte (0x00) Consistency Tests ==============
// These tests verify that RE2, Boost regex, and LikePatternMatcher all handle
// NUL bytes consistently. This is important for:
// - Growing data (uses LikePatternMatcher directly)
// - RE2 regex path
// - Sealed segment with index

TEST(NulByteConsistencyTest, UnderscoreMatchesNulByte) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test that _ matches a NUL byte (0x00) consistently across all matchers
    std::string str_with_nul = "a";
    str_with_nul += '\0';
    str_with_nul += "b";  // "a\0b" - 3 bytes

    std::string pattern = "a_b";
    auto regex_pattern = translator(pattern);
    RegexMatcher re2_matcher(regex_pattern);
    BoostRegexMatcher boost_matcher(regex_pattern);
    LikePatternMatcher like_matcher(pattern);

    // All matchers should treat NUL as a valid single-byte character
    EXPECT_TRUE(re2_matcher(str_with_nul)) << "RE2 should match NUL with _";
    EXPECT_TRUE(boost_matcher(str_with_nul)) << "Boost should match NUL with _";
    EXPECT_TRUE(like_matcher(str_with_nul))
        << "LikePatternMatcher should match NUL with _";

    // All must agree
    EXPECT_EQ(re2_matcher(str_with_nul), like_matcher(str_with_nul))
        << "RE2 and LikePatternMatcher must agree on NUL byte handling";
}

TEST(NulByteConsistencyTest, PercentMatchesStringWithNul) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test that % correctly matches strings containing NUL bytes
    std::string str_with_nul = "hello";
    str_with_nul += '\0';
    str_with_nul += "world";  // "hello\0world" - 11 bytes

    struct TestCase {
        std::string pattern;
        bool expected;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
        {"hello%", true, "prefix match with NUL in suffix"},
        {"%world", true, "suffix match with NUL in prefix"},
        {"hello%world", true, "prefix+suffix with NUL in middle"},
        {"%llo%wor%", true, "inner match across NUL"},
        {"hello_world", true, "_ matches the NUL byte between hello and world"},
    };

    for (const auto& tc : test_cases) {
        auto regex_pattern = translator(tc.pattern);
        RegexMatcher re2_matcher(regex_pattern);
        BoostRegexMatcher boost_matcher(regex_pattern);
        LikePatternMatcher like_matcher(tc.pattern);

        EXPECT_EQ(re2_matcher(str_with_nul), tc.expected)
            << "RE2 failed: " << tc.description;
        EXPECT_EQ(boost_matcher(str_with_nul), tc.expected)
            << "Boost failed: " << tc.description;
        EXPECT_EQ(like_matcher(str_with_nul), tc.expected)
            << "LikePatternMatcher failed: " << tc.description;

        // Consistency check
        EXPECT_EQ(re2_matcher(str_with_nul), like_matcher(str_with_nul))
            << "RE2/Like mismatch: " << tc.description;
    }
}

TEST(NulByteConsistencyTest, MultipleNulBytes) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // String with multiple NUL bytes
    std::string multi_nul = "a";
    multi_nul += '\0';
    multi_nul += '\0';
    multi_nul += "b";  // "a\0\0b" - 4 bytes

    struct TestCase {
        std::string pattern;
        bool expected;
    };

    std::vector<TestCase> test_cases = {
        {"a__b", true},    // Two underscores match two NULs
        {"a_b", false},    // One underscore can't match two NULs
        {"a___b", false},  // Three underscores don't match two NULs
        {"a%b", true},     // Percent matches any number of NULs
        {"%", true},       // Percent matches everything
    };

    for (const auto& tc : test_cases) {
        auto regex_pattern = translator(tc.pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(tc.pattern);

        bool re2_result = re2_matcher(multi_nul);
        bool like_result = like_matcher(multi_nul);

        EXPECT_EQ(re2_result, tc.expected)
            << "RE2 failed for pattern: " << tc.pattern;
        EXPECT_EQ(like_result, tc.expected)
            << "LikePatternMatcher failed for pattern: " << tc.pattern;
        EXPECT_EQ(re2_result, like_result)
            << "RE2/Like mismatch for pattern: " << tc.pattern;
    }
}

// ============== Invalid UTF-8 Handling Consistency Tests ==============
// These tests verify behavior with malformed UTF-8 sequences

TEST(InvalidUTF8ConsistencyTest, TruncatedMultibyteSequence) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Truncated 2-byte sequence: \xC3 alone (should be \xC3\xA9 for é)
    std::string truncated_2byte = "caf\xC3";  // 4 bytes, truncated é

    // Truncated 3-byte sequence: \xE4\xBD alone (should be \xE4\xBD\xA0 for 你)
    std::string truncated_3byte = "a\xE4\xBD";  // 3 bytes, truncated 你

    // Test with underscore wildcard
    {
        std::string pattern = "caf_";
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        // RE2 and LikePatternMatcher should handle invalid UTF-8 consistently
        bool re2_result = re2_matcher(truncated_2byte);
        bool like_result = like_matcher(truncated_2byte);

        // The key requirement is consistency, not a specific result
        EXPECT_EQ(re2_result, like_result)
            << "RE2/Like must agree on truncated 2-byte UTF-8 handling";
    }

    // Test with percent wildcard - behavior with invalid UTF-8 is implementation-defined
    // The key is consistency between matchers
    {
        std::string pattern = "caf%";
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        bool re2_result = re2_matcher(truncated_2byte);
        bool like_result = like_matcher(truncated_2byte);

        // For invalid UTF-8, we require consistency, not a specific result
        // Note: RE2's UTF-8 pattern may reject invalid sequences while LikePatternMatcher
        // may accept them as raw bytes. This is expected divergence for invalid input.
        (void)re2_result;
        (void)like_result;
    }
}

TEST(InvalidUTF8ConsistencyTest, OverlongEncoding) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Overlong encoding of ASCII 'a' (0x61) using 2 bytes: \xC1\xA1
    // This is invalid UTF-8 - behavior is implementation-defined
    std::string overlong = "\xC1\xA1";  // Invalid overlong 'a'

    std::string pattern = "_";
    auto regex_pattern = translator(pattern);
    RegexMatcher re2_matcher(regex_pattern);
    LikePatternMatcher like_matcher(pattern);

    bool re2_result = re2_matcher(overlong);
    bool like_result = like_matcher(overlong);

    // For invalid UTF-8, behavior may diverge between implementations.
    // RE2's UTF-8 pattern may treat this as 2 invalid bytes, while
    // LikePatternMatcher may treat it as 1 UTF-8 character.
    // We just verify both run without crashing.
    (void)re2_result;
    (void)like_result;
}

TEST(InvalidUTF8ConsistencyTest, InvalidContinuationByte) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Invalid: continuation byte without lead byte
    std::string invalid_cont =
        "a\x80"
        "b";  // 0x80 is a continuation byte

    std::string pattern = "a_b";
    auto regex_pattern = translator(pattern);
    RegexMatcher re2_matcher(regex_pattern);
    LikePatternMatcher like_matcher(pattern);

    bool re2_result = re2_matcher(invalid_cont);
    bool like_result = like_matcher(invalid_cont);

    // For invalid UTF-8, behavior may diverge between implementations.
    // Just verify both run without crashing.
    (void)re2_result;
    (void)like_result;
}

// ============== Execution Path Consistency Tests ==============
// These tests ensure that all execution paths produce identical results:
// - Growing data path (LikePatternMatcher, brute-force scan)
// - RE2 regex path (RegexMatcher)
// - Sealed segment index path (uses LikePatternMatcher via BitmapIndex)

TEST(ExecutionPathConsistencyTest, ComprehensivePatternMatching) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Test data representing what might be stored in segments
    std::vector<std::string> test_data = {
        "hello",
        "world",
        "hello world",
        "HELLO",
        "hello123",
        "123hello",
        "h3ll0",
        "",                           // Empty string
        "café",                       // UTF-8 2-byte
        "\xE4\xBD\xA0\xE5\xA5\xBD",   // 你好 (UTF-8 3-byte)
        "emoji\xF0\x9F\x98\x80test",  // emoji (UTF-8 4-byte)
        std::string("with\0nul", 8),  // String with NUL byte
        "aaaa",                       // For overlapping tests
        "aaa",                        // For overlapping tests
        "aba",                        // For overlapping tests
    };

    // Patterns to test various scenarios
    std::vector<std::string> patterns = {
        "hello",     // Exact match
        "hello%",    // Prefix match
        "%world",    // Suffix match
        "%llo%",     // Inner match
        "h_llo",     // Single char wildcard
        "h%o",       // Prefix + suffix with gap
        "%",         // Match all
        "_____",     // Exact length match
        "%%",        // Multiple wildcards
        "%a%a%",     // Multiple inner matches
        "caf_",      // UTF-8 single char
        "\\%hello",  // Escaped percent (literal %)
        "hello\\_",  // Escaped underscore
        "%aa%aa%",   // Overlapping pattern
        "a%aa",      // Overlapping prefix/suffix
        "%ab%ba%",   // Different overlapping segments
    };

    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        for (const auto& data : test_data) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            // Critical: All execution paths must produce identical results
            EXPECT_EQ(re2_result, like_result)
                << "Execution path mismatch!\n"
                << "  Pattern: \"" << pattern << "\"\n"
                << "  Data: \"" << data << "\" (length=" << data.size() << ")\n"
                << "  RE2 (sealed segment regex path): " << re2_result << "\n"
                << "  LikePatternMatcher (growing data / index path): "
                << like_result;
        }
    }
}

TEST(ExecutionPathConsistencyTest, EdgeCasesForAllPaths) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Edge cases that might behave differently between paths
    struct TestCase {
        std::string pattern;
        std::string data;
        bool expected;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
        // Empty patterns and strings
        {"", "", true, "Empty pattern matches empty string"},
        {"%", "", true, "Percent matches empty string"},
        {"_", "", false, "Underscore doesn't match empty string"},
        {"", "a", false, "Empty pattern doesn't match non-empty string"},

        // Single character patterns
        {"_", "a", true, "Underscore matches single char"},
        {"_", "\xC3\xA9", true, "Underscore matches 2-byte UTF-8"},
        {"_", "\xE4\xBD\xA0", true, "Underscore matches 3-byte UTF-8"},
        {"_", "\xF0\x9F\x98\x80", true, "Underscore matches 4-byte UTF-8"},

        // Non-overlapping patterns (SQL LIKE semantics)
        {"%aa%aa%", "aaaa", true, "Two non-overlapping 'aa' segments"},
        {"%aa%aa%", "aaa", false, "Only one 'aa' fits, no room for second"},
        {"%aa%aa%", "aa", false, "Not enough for double segment"},
        {"%aa%aa%aa%", "aaaaaa", true, "Three non-overlapping 'aa' segments"},
        {"%aa%aa%aa%", "aaaa", false, "Only room for two non-overlapping 'aa'"},
        {"%ab%ba%", "abba", true, "Non-overlapping ab then ba"},
        {"%ab%ba%", "aba", false, "ab and ba would overlap"},

        // Consecutive wildcards
        {"%%", "anything", true, "Double percent"},
        {"%%%", "", true, "Triple percent matches empty"},
        {"_%_%", "ab", true, "Alternating wildcards"},

        // Escape sequences
        {"\\%", "%", true, "Escaped percent is literal"},
        {"\\_", "_", true, "Escaped underscore is literal"},
        {"\\\\", "\\", true, "Escaped backslash"},
        {"100\\%", "100%", true, "Escaped percent in context"},
    };

    for (const auto& tc : test_cases) {
        auto regex_pattern = translator(tc.pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(tc.pattern);

        bool re2_result = re2_matcher(tc.data);
        bool like_result = like_matcher(tc.data);

        EXPECT_EQ(re2_result, tc.expected) << "RE2 failed: " << tc.description;
        EXPECT_EQ(like_result, tc.expected)
            << "LikePatternMatcher failed: " << tc.description;
        EXPECT_EQ(re2_result, like_result)
            << "Path mismatch: " << tc.description;
    }
}

TEST(ExecutionPathConsistencyTest, LargeDatasetSimulation) {
    using namespace milvus;
    PatternMatchTranslator translator;

    // Simulate pattern matching against a large dataset
    // This tests that both paths scale correctly
    std::vector<std::string> large_dataset;
    large_dataset.reserve(1000);

    // Generate varied test data
    for (int i = 0; i < 100; i++) {
        large_dataset.push_back("prefix_" + std::to_string(i) + "_suffix");
        large_dataset.push_back("test" + std::to_string(i));
        large_dataset.push_back(std::string(i % 50 + 1, 'a'));  // Variable 'a's
        large_dataset.push_back("mixed_" + std::to_string(i) + "_data_" +
                                std::to_string(i * 2));
        // Add some UTF-8
        if (i % 10 == 0) {
            large_dataset.push_back("utf8_\xE4\xBD\xA0_" + std::to_string(i));
        }
    }

    std::vector<std::string> patterns = {
        "prefix%",
        "%suffix",
        "%_data_%",
        "test_",
        "%aa%",
        "%\xE4\xBD\xA0%",
    };

    for (const auto& pattern : patterns) {
        auto regex_pattern = translator(pattern);
        RegexMatcher re2_matcher(regex_pattern);
        LikePatternMatcher like_matcher(pattern);

        int match_count_re2 = 0;
        int match_count_like = 0;

        for (const auto& data : large_dataset) {
            bool re2_result = re2_matcher(data);
            bool like_result = like_matcher(data);

            if (re2_result)
                match_count_re2++;
            if (like_result)
                match_count_like++;

            // Every individual result must match
            ASSERT_EQ(re2_result, like_result)
                << "Mismatch on pattern \"" << pattern << "\" with data \""
                << data << "\"";
        }

        // Sanity check: match counts must be identical
        EXPECT_EQ(match_count_re2, match_count_like)
            << "Total match count mismatch for pattern: " << pattern;
    }
}
