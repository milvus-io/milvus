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
#include <string>
#include <utility>

#include "exec/expression/UnaryExpr.h"

using namespace milvus::exec;

class SplitAtFirstSlashDigitTest : public ::testing::Test {};

TEST_F(SplitAtFirstSlashDigitTest, NoSlash) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("abc");
    EXPECT_EQ(result.first, "abc");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashWithoutDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("a/b/c");
    EXPECT_EQ(result.first, "a/b/c");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashWithDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("abc/123");
    EXPECT_EQ(result.first, "abc");
    EXPECT_EQ(result.second, "/123");
}

TEST_F(SplitAtFirstSlashDigitTest, MultipleSlashesFirstWithDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("a/1/b/2");
    EXPECT_EQ(result.first, "a");
    EXPECT_EQ(result.second, "/1/b/2");
}

TEST_F(SplitAtFirstSlashDigitTest, MultipleSlashesSecondWithDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("a/b/1/c");
    EXPECT_EQ(result.first, "a/b");
    EXPECT_EQ(result.second, "/1/c");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashDigitAtBeginning) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("/123abc");
    EXPECT_EQ(result.first, "");
    EXPECT_EQ(result.second, "/123abc");
}

TEST_F(SplitAtFirstSlashDigitTest, EmptyString) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("");
    EXPECT_EQ(result.first, "");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, JustSlashDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("/1");
    EXPECT_EQ(result.first, "");
    EXPECT_EQ(result.second, "/1");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashAtEndWithoutDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("abc/");
    EXPECT_EQ(result.first, "abc/");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, PathLikeInput) {
    auto result =
        PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("/foo/bar/123");
    EXPECT_EQ(result.first, "/foo/bar");
    EXPECT_EQ(result.second, "/123");
}

TEST_F(SplitAtFirstSlashDigitTest, JsonPointerPath) {
    // Typical JSON pointer path like /data/items/0/name
    auto result =
        PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("/data/items/0/name");
    EXPECT_EQ(result.first, "/data/items");
    EXPECT_EQ(result.second, "/0/name");
}

TEST_F(SplitAtFirstSlashDigitTest, OnlySlash) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("/");
    EXPECT_EQ(result.first, "/");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashFollowedByLetter) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("/abc");
    EXPECT_EQ(result.first, "/abc");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, DigitWithoutSlash) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("abc123");
    EXPECT_EQ(result.first, "abc123");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashZero) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("path/0");
    EXPECT_EQ(result.first, "path");
    EXPECT_EQ(result.second, "/0");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashNine) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("path/9");
    EXPECT_EQ(result.first, "path");
    EXPECT_EQ(result.second, "/9");
}

// ============== Additional Edge Case Tests ==============

TEST_F(SplitAtFirstSlashDigitTest, AllDigits0To9) {
    // Test all ASCII digits to ensure locale-independent behavior
    for (char d = '0'; d <= '9'; ++d) {
        std::string input = std::string("path/") + d;
        auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit(input);
        EXPECT_EQ(result.first, "path") << "Failed for digit: " << d;
        EXPECT_EQ(result.second, std::string("/") + d)
            << "Failed for digit: " << d;
    }
}

TEST_F(SplitAtFirstSlashDigitTest, NonAsciiDigits) {
    // Non-ASCII digit characters should NOT be treated as digits
    // Arabic-Indic digits (U+0660-U+0669) - if passed as UTF-8
    // These should not trigger a split
    auto result =
        PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("path/\xD9\xA0");
    EXPECT_EQ(result.first, "path/\xD9\xA0");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, MultipleDigitsAfterSlash) {
    auto result =
        PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("a/123/b/456");
    EXPECT_EQ(result.first, "a");
    EXPECT_EQ(result.second, "/123/b/456");
}

TEST_F(SplitAtFirstSlashDigitTest, ConsecutiveSlashes) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("a//1");
    EXPECT_EQ(result.first, "a/");
    EXPECT_EQ(result.second, "/1");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashSlashDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("//0");
    EXPECT_EQ(result.first, "/");
    EXPECT_EQ(result.second, "/0");
}

TEST_F(SplitAtFirstSlashDigitTest, VeryLongPath) {
    std::string long_prefix(1000, 'a');
    std::string input = long_prefix + "/123";
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit(input);
    EXPECT_EQ(result.first, long_prefix);
    EXPECT_EQ(result.second, "/123");
}

TEST_F(SplitAtFirstSlashDigitTest, SpecialCharactersBeforeSlashDigit) {
    auto result =
        PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("path with spaces/123");
    EXPECT_EQ(result.first, "path with spaces");
    EXPECT_EQ(result.second, "/123");
}

TEST_F(SplitAtFirstSlashDigitTest, UnicodeBeforeSlashDigit) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("路径/123");
    EXPECT_EQ(result.first, "路径");
    EXPECT_EQ(result.second, "/123");
}

TEST_F(SplitAtFirstSlashDigitTest, BackslashNotTreatedAsSlash) {
    // Backslash should not trigger split
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("path\\123");
    EXPECT_EQ(result.first, "path\\123");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, MixedSlashAndBackslash) {
    auto result =
        PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("a\\b/1\\c/2");
    EXPECT_EQ(result.first, "a\\b");
    EXPECT_EQ(result.second, "/1\\c/2");
}

TEST_F(SplitAtFirstSlashDigitTest, SlashAtVeryEnd) {
    auto result =
        PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("no_digit_after/");
    EXPECT_EQ(result.first, "no_digit_after/");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, OnlyDigits) {
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit("12345");
    EXPECT_EQ(result.first, "12345");
    EXPECT_EQ(result.second, "");
}

TEST_F(SplitAtFirstSlashDigitTest, NestedJsonArrayPath) {
    // Real-world JSON pointer: /data/items/0/subarray/1/value
    auto result = PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit(
        "/data/items/0/subarray/1/value");
    EXPECT_EQ(result.first, "/data/items");
    EXPECT_EQ(result.second, "/0/subarray/1/value");
}
