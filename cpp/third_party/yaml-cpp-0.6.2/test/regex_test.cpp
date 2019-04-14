#include "gtest/gtest.h"
#include "regex_yaml.h"
#include "stream.h"

using YAML::RegEx;
using YAML::Stream;

namespace {
const auto MIN_CHAR = Stream::eof() + 1;

TEST(RegExTest, Empty) {
  RegEx empty;
  EXPECT_TRUE(empty.Matches(std::string()));
  EXPECT_EQ(0, empty.Match(std::string()));
  for (int i = MIN_CHAR; i < 128; ++i) {
    auto str = std::string(1, char(i));
    EXPECT_FALSE(empty.Matches(str));
    EXPECT_EQ(-1, empty.Match(str));
  }
}

TEST(RegExTest, Range) {
  for (int i = MIN_CHAR; i < 128; ++i) {
    for (int j = MIN_CHAR; j < 128; ++j) {
      RegEx ex((char)i, (char)j);
      for (int k = MIN_CHAR; k < 128; ++k) {
        auto str = std::string(1, char(k));
        if (i <= k && k <= j) {
          EXPECT_TRUE(ex.Matches(str));
          EXPECT_EQ(1, ex.Match(str));
        } else {
          EXPECT_FALSE(ex.Matches(str));
          EXPECT_EQ(-1, ex.Match(str));
        }
      }
    }
  }
}

TEST(RegExTest, EmptyString) {
  RegEx ex = RegEx(std::string());
  EXPECT_TRUE(ex.Matches(std::string()));
  EXPECT_EQ(0, ex.Match(std::string()));

  // Matches anything, unlike RegEx()!
  EXPECT_TRUE(ex.Matches(std::string("hello")));
  EXPECT_EQ(0, ex.Match(std::string("hello")));
}

TEST(RegExTest, SingleCharacterString) {
  for (int i = MIN_CHAR; i < 128; ++i) {
    RegEx ex(std::string(1, (char)i));
    for (int j = MIN_CHAR; j < 128; ++j) {
      auto str = std::string(1, char(j));
      if (j == i) {
        EXPECT_TRUE(ex.Matches(str));
        EXPECT_EQ(1, ex.Match(str));
        // Match at start of string only!
        std::string prefixed =
            std::string(1, i + 1) + std::string("prefix: ") + str;
        EXPECT_FALSE(ex.Matches(prefixed));
        EXPECT_EQ(-1, ex.Match(prefixed));
      } else {
        EXPECT_FALSE(ex.Matches(str));
        EXPECT_EQ(-1, ex.Match(str));
      }
    }
  }
}

TEST(RegExTest, MultiCharacterString) {
  RegEx ex(std::string("ab"));

  EXPECT_FALSE(ex.Matches(std::string("a")));
  EXPECT_EQ(-1, ex.Match(std::string("a")));

  EXPECT_TRUE(ex.Matches(std::string("ab")));
  EXPECT_EQ(2, ex.Match(std::string("ab")));
  EXPECT_TRUE(ex.Matches(std::string("abba")));
  EXPECT_EQ(2, ex.Match(std::string("abba")));

  // match at start of string only!
  EXPECT_FALSE(ex.Matches(std::string("baab")));
  EXPECT_EQ(-1, ex.Match(std::string("baab")));
}

TEST(RegExTest, OperatorNot) {
  RegEx ex = !RegEx(std::string("ab"));

  EXPECT_TRUE(ex.Matches(std::string("a")));
  EXPECT_EQ(1, ex.Match(std::string("a")));

  EXPECT_FALSE(ex.Matches(std::string("ab")));
  EXPECT_EQ(-1, ex.Match(std::string("ab")));
  EXPECT_FALSE(ex.Matches(std::string("abba")));
  EXPECT_EQ(-1, ex.Match(std::string("abba")));

  // match at start of string only!
  EXPECT_TRUE(ex.Matches(std::string("baab")));
  // Operator not causes only one character to be matched.
  EXPECT_EQ(1, ex.Match(std::string("baab")));
}

TEST(RegExTest, OperatorOr) {
  for (int i = MIN_CHAR; i < 127; ++i) {
    for (int j = i + 1; j < 128; ++j) {
      auto iStr = std::string(1, char(i));
      auto jStr = std::string(1, char(j));
      RegEx ex1 = RegEx(iStr) || RegEx(jStr);
      RegEx ex2 = RegEx(jStr) || RegEx(iStr);

      for (int k = MIN_CHAR; k < 128; ++k) {
        auto str = std::string(1, char(k));
        if (i == k || j == k) {
          EXPECT_TRUE(ex1.Matches(str));
          EXPECT_TRUE(ex2.Matches(str));
          EXPECT_EQ(1, ex1.Match(str));
          EXPECT_EQ(1, ex2.Match(str));
        } else {
          EXPECT_FALSE(ex1.Matches(str));
          EXPECT_FALSE(ex2.Matches(str));
          EXPECT_EQ(-1, ex1.Match(str));
          EXPECT_EQ(-1, ex2.Match(str));
        }
      }
    }
  }
}

TEST(RegExTest, OperatorOrShortCircuits) {
  RegEx ex1 = RegEx(std::string("aaaa")) || RegEx(std::string("aa"));
  RegEx ex2 = RegEx(std::string("aa")) || RegEx(std::string("aaaa"));

  EXPECT_TRUE(ex1.Matches(std::string("aaaaa")));
  EXPECT_EQ(4, ex1.Match(std::string("aaaaa")));

  EXPECT_TRUE(ex2.Matches(std::string("aaaaa")));
  EXPECT_EQ(2, ex2.Match(std::string("aaaaa")));
}

TEST(RegExTest, OperatorAnd) {
  RegEx emptySet = RegEx('a') && RegEx();
  EXPECT_FALSE(emptySet.Matches(std::string("a")));
}

TEST(RegExTest, OperatorAndShortCircuits) {
  RegEx ex1 = RegEx(std::string("aaaa")) && RegEx(std::string("aa"));
  RegEx ex2 = RegEx(std::string("aa")) && RegEx(std::string("aaaa"));

  EXPECT_TRUE(ex1.Matches(std::string("aaaaa")));
  EXPECT_EQ(4, ex1.Match(std::string("aaaaa")));

  EXPECT_TRUE(ex2.Matches(std::string("aaaaa")));
  EXPECT_EQ(2, ex2.Match(std::string("aaaaa")));
}

TEST(RegExTest, OperatorPlus) {
  RegEx ex = RegEx(std::string("hello ")) + RegEx(std::string("there"));

  EXPECT_TRUE(ex.Matches(std::string("hello there")));
  EXPECT_FALSE(ex.Matches(std::string("hello ")));
  EXPECT_FALSE(ex.Matches(std::string("there")));
  EXPECT_EQ(11, ex.Match(std::string("hello there")));
}

TEST(RegExTest, StringOr) {
  std::string str = "abcde";
  RegEx ex = RegEx(str, YAML::REGEX_OR);

  for (size_t i = 0; i < str.size(); ++i) {
    EXPECT_TRUE(ex.Matches(str.substr(i, 1)));
    EXPECT_EQ(1, ex.Match(str.substr(i, 1)));
  }

  EXPECT_EQ(1, ex.Match(str));
}
}
