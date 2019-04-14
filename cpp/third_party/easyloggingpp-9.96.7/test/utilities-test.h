
#ifndef UTILITIES_TEST_H_
#define UTILITIES_TEST_H_

#include "test.h"

TEST(UtilitiesTest, SafeDelete) {
    int* i = new int(12);
    ASSERT_TRUE(i != nullptr);
    safeDelete(i);
    ASSERT_EQ(nullptr, i);
}

#endif // UTILITIES_TEST_H_
