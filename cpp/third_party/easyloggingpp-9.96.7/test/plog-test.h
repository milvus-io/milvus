#ifndef PLOG_TEST_H
#define PLOG_TEST_H

#include "test.h"

TEST(PLogTest, WriteLog) {
    std::fstream file("/tmp/a/file/that/does/not/exist.txt", std::fstream::in);
    if (file.is_open()) {
        // We dont expect to open file
        FAIL();
    }
    PLOG(INFO) << "This is plog";
    std::string expected = BUILD_STR(getDate() << " This is plog: No such file or directory [2]\n");
    std::string actual = tail(1);
    EXPECT_EQ(expected, actual);
}

#endif // PLOG_TEST_H
