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
