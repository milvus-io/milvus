#ifndef MACROS_TEST_H_
#define MACROS_TEST_H_

#include "test.h"

TEST(MacrosTest, VaLength) {
    EXPECT_EQ(10, el_getVALength("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));
    EXPECT_EQ(9, el_getVALength("a", "b", "c", "d", "e", "f", "g", "h", "i"));
    EXPECT_EQ(8, el_getVALength("a", "b", "c", "d", "e", "f", "g", "h"));
    EXPECT_EQ(7, el_getVALength("a", "b", "c", "d", "e", "f", "g"));
    EXPECT_EQ(6, el_getVALength("a", "b", "c", "d", "e", "f"));
    EXPECT_EQ(5, el_getVALength("a", "b", "c", "d", "e"));
    EXPECT_EQ(4, el_getVALength("a", "b", "c", "d"));
    EXPECT_EQ(3, el_getVALength("a", "b", "c"));
    EXPECT_EQ(2, el_getVALength("a", "b"));
    EXPECT_EQ(1, el_getVALength("a"));
}

#endif // MACROS_TEST_H_
