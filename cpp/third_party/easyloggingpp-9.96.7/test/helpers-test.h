
#ifndef HELPERS_TEST_H_
#define HELPERS_TEST_H_

#include "test.h"
#include <vector>

TEST(HelpersTest, ConvertTemplateToStdString) {
    std::vector<int> vecInt;
    vecInt.push_back(1);
    vecInt.push_back(2);
    vecInt.push_back(3);
    vecInt.push_back(4);
    std::string strVecInt = el::Helpers::convertTemplateToStdString(vecInt);
    EXPECT_EQ("[1, 2, 3, 4]", strVecInt);
}

#endif // HELPERS_TEST_H_
