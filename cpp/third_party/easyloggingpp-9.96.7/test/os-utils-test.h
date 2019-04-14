#ifndef OS_UTILS_TEST_H
#define OS_UTILS_TEST_H

#include "test.h"

#if ELPP_OS_UNIX && !ELPP_OS_EMSCRIPTEN
// this doesn't make any sense under emscripten; you can't shell out
TEST(OSUtilsTest, GetBashOutput) {
    const char* bashCommand = "echo 'test'";
    std::string bashResult = OS::getBashOutput(bashCommand);
    EXPECT_EQ("test", bashResult);
}
#endif

TEST(OSUtilsTest, GetEnvironmentVariable) {
    std::string variable = OS::getEnvironmentVariable("PATH", "pathResult");
    EXPECT_FALSE(variable.empty());
}
#endif // OS_UTILS_TEST_H
