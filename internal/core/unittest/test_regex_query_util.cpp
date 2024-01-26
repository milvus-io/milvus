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

TEST(TranslatePatternMatchToRegexTest, SimplePatternWithPercent) {
    std::string pattern = "abc%";
    std::string result = milvus::TranslatePatternMatchToRegex(pattern);
    EXPECT_EQ(result, "abc.*");
}

TEST(TranslatePatternMatchToRegexTest, PatternWithUnderscore) {
    std::string pattern = "a_c";
    std::string result = milvus::TranslatePatternMatchToRegex(pattern);
    EXPECT_EQ(result, "a.c");
}

TEST(TranslatePatternMatchToRegexTest, PatternWithSpecialCharacters) {
    std::string pattern = "a\\%b\\_c";
    std::string result = milvus::TranslatePatternMatchToRegex(pattern);
    EXPECT_EQ(result, "a\\%b\\_c");
}

TEST(TranslatePatternMatchToRegexTest,
     PatternWithMultiplePercentAndUnderscore) {
    std::string pattern = "%a_b%";
    std::string result = milvus::TranslatePatternMatchToRegex(pattern);
    EXPECT_EQ(result, ".*a.b.*");
}

TEST(TranslatePatternMatchToRegexTest, PatternWithRegexChar) {
    std::string pattern = "abc*def.ghi+";
    std::string result = milvus::TranslatePatternMatchToRegex(pattern);
    EXPECT_EQ(result, "abc\\*def\\.ghi\\+");
}
