#ifndef STRING_UTILS_TEST_H
#define STRING_UTILS_TEST_H

#include "test.h"

TEST(StringUtilsTest, WildCardMatch) {
    EXPECT_TRUE(Str::wildCardMatch("main", "m*"));
    EXPECT_TRUE(Str::wildCardMatch("mei.cpp", "m*cpp"));
    EXPECT_TRUE(Str::wildCardMatch("me.cpp", "me.cpp"));
    EXPECT_TRUE(Str::wildCardMatch("me.cpp", "m?.cpp"));
    EXPECT_TRUE(Str::wildCardMatch("m/f.cpp", "m??.cpp"));
    EXPECT_TRUE(Str::wildCardMatch("", "*"));
    EXPECT_FALSE(Str::wildCardMatch("", "?"));
    EXPECT_TRUE(Str::wildCardMatch("fastquery--and anything after or before", "*****************fast*****query*****"));
    EXPECT_FALSE(Str::wildCardMatch("some thing not matching", "some * matching all"));
}

TEST(StringUtilsTest, Trim) {
    std::string strLeftWhiteSpace(" string 1");
    std::string strLeftRightWhiteSpace(" string 2 ");
    std::string strRightWhiteSpace("string 3 ");
    std::string strLeftRightWhiteSpaceWithTabAndNewLine("  string 4 \t\n");
    EXPECT_EQ("string 1", Str::trim(strLeftWhiteSpace));
    EXPECT_EQ("string 2", Str::trim(strLeftRightWhiteSpace));
    EXPECT_EQ("string 3", Str::trim(strRightWhiteSpace));
    EXPECT_EQ("string 4", Str::trim(strLeftRightWhiteSpaceWithTabAndNewLine));
}

TEST(StringUtilsTest, StartsAndEndsWith) {
    EXPECT_TRUE(Str::startsWith("Dotch this", "Dotch"));
    EXPECT_FALSE(Str::startsWith("Dotch this", "dotch"));
    EXPECT_FALSE(Str::startsWith("    Dotch this", "dotch"));
    EXPECT_TRUE(Str::endsWith("Dotch this", "this"));
    EXPECT_FALSE(Str::endsWith("Dotch this", "This"));
}

TEST(StringUtilsTest, ReplaceAll) {
    std::string str = "This is cool";
    char replaceWhat = 'o';
    char replaceWith = '0';
    EXPECT_EQ("This is c00l", Str::replaceAll(str, replaceWhat, replaceWith));
}

TEST(StringUtilsTest, ToUpper) {
    std::string str = "This iS c0ol";
    EXPECT_EQ("THIS IS C0OL", Str::toUpper(str));
    str = "enabled = ";
    EXPECT_EQ("ENABLED = ", Str::toUpper(str));
}

TEST(StringUtilsTest, CStringEq) {
    EXPECT_TRUE(Str::cStringEq("this", "this"));
    EXPECT_FALSE(Str::cStringEq(nullptr, "this"));
    EXPECT_TRUE(Str::cStringEq(nullptr, nullptr));
}

TEST(StringUtilsTest, CStringCaseEq) {
    EXPECT_TRUE(Str::cStringCaseEq("this", "This"));
    EXPECT_TRUE(Str::cStringCaseEq("this", "this"));
    EXPECT_TRUE(Str::cStringCaseEq(nullptr, nullptr));
    EXPECT_FALSE(Str::cStringCaseEq(nullptr, "nope"));
}

TEST(StringUtilsTest, Contains) {
    EXPECT_TRUE(Str::contains("the quick brown fox jumped over the lazy dog", 'a'));
    EXPECT_FALSE(Str::contains("the quick brown fox jumped over the lazy dog", '9'));
}

TEST(StringUtilsTest, ReplaceFirstWithEscape) {
    el::base::type::string_t str = ELPP_LITERAL("Rolling in the deep");
    Str::replaceFirstWithEscape(str, ELPP_LITERAL("Rolling"), ELPP_LITERAL("Swimming"));
    EXPECT_EQ(ELPP_LITERAL("Swimming in the deep"), str);
    str = ELPP_LITERAL("this is great and this is not");
    Str::replaceFirstWithEscape(str, ELPP_LITERAL("this is"), ELPP_LITERAL("that was"));
    EXPECT_EQ(ELPP_LITERAL("that was great and this is not"), str);
    str = ELPP_LITERAL("%this is great and this is not");
    Str::replaceFirstWithEscape(str, ELPP_LITERAL("this is"), ELPP_LITERAL("that was"));
    EXPECT_EQ(ELPP_LITERAL("this is great and that was not"), str);
    str = ELPP_LITERAL("%datetime %level %msg");
    Str::replaceFirstWithEscape(str, ELPP_LITERAL("%level"), LevelHelper::convertToString(Level::Info));
    EXPECT_EQ(ELPP_LITERAL("%datetime INFO %msg"), str);
}

TEST(StringUtilsTest, AddToBuff) {
    char buf[100];
    char* bufLim = buf + 100;
    char* buffPtr = buf;

    buffPtr = Str::addToBuff("The quick brown fox", buffPtr, bufLim);
    EXPECT_STREQ("The quick brown fox", buf);
    buffPtr = Str::addToBuff(" jumps over the lazy dog", buffPtr, bufLim);
    EXPECT_STREQ("The quick brown fox jumps over the lazy dog", buf);
}

TEST(StringUtilsTest, ConvertAndAddToBuff) {
    char buf[100];
    char* bufLim = buf + 100;
    char* buffPtr = buf;

    buffPtr = Str::addToBuff("Today is lets say ", buffPtr, bufLim);
    buffPtr = Str::convertAndAddToBuff(5, 1, buffPtr, bufLim);
    buffPtr = Str::addToBuff(" but we write it as ", buffPtr, bufLim);
    buffPtr = Str::convertAndAddToBuff(5, 2, buffPtr, bufLim);
    EXPECT_STREQ("Today is lets say 5 but we write it as 05", buf);
}

#endif // STRING_UTILS_TEST_H
