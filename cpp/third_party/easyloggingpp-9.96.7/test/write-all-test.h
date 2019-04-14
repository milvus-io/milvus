
#ifndef WRITE_ALL_TEST_H_
#define WRITE_ALL_TEST_H_

#include "test.h"

TEST(WriteAllTest, Entry) {
    reconfigureLoggersForTest();
}

#define TEST_LEVEL(l, name) \
TEST(WriteAllTest, l) {\
    std::string s;\
    LOG(l) << name << " 1";\
    s = BUILD_STR(getDate() << " " << name << " 1\n");\
    EXPECT_EQ(s, tail(1));\
    LOG_IF(true, l) << name << " 2";\
    s = BUILD_STR(getDate() << " " << name << " 1\n"\
               << getDate() << " " << name << " 2\n");\
    EXPECT_EQ(s, tail(2));\
    LOG_IF(true, l) << name << " 3";\
    s = BUILD_STR(getDate() << " " << name << " 3\n");\
    EXPECT_EQ(s, tail(1));\
    LOG_IF(false, l) << "SHOULD NOT LOG";\
    s = BUILD_STR(getDate() << " " << name << " 3\n");\
    EXPECT_EQ(s, tail(1));\
    LOG_EVERY_N(1, l) << name << " every n=1";\
    s = BUILD_STR(getDate() << " " << name << " every n=1\n");\
    LOG_AFTER_N(1, l) << name << " after n=1";\
    s = BUILD_STR(getDate() << " " << name << " after n=1\n");\
    LOG_N_TIMES(2, l) << name << " n times=2";\
    s = BUILD_STR(getDate() << " " << name << " n times=2\n");\
    EXPECT_EQ(s, tail(1));\
}

TEST_LEVEL(DEBUG, "Debug")
TEST_LEVEL(INFO, "Info")
TEST_LEVEL(ERROR, "Error")
TEST_LEVEL(WARNING, "Warning")
TEST_LEVEL(FATAL, "Fatal")
TEST_LEVEL(TRACE, "Trace")

TEST(WriteAllTest, VERBOSE) {
    Configurations cOld(*Loggers::getLogger("default")->configurations());
    Loggers::reconfigureAllLoggers(ConfigurationType::Format, "%datetime{%a %b %d, %H:%m} %level-%vlevel %msg");

    el::Loggers::addFlag(el::LoggingFlag::AllowVerboseIfModuleNotSpecified); // Accept all verbose levels; we already have vmodules!

    std::string s;
    for (int i = 1; i <= 6; ++i)
        VLOG_EVERY_N(2, 2) << "every n=" << i;

    s = BUILD_STR(getDate() << " VERBOSE-2 every n=2\n"
               << getDate() << " VERBOSE-2 every n=4\n"
               << getDate() << " VERBOSE-2 every n=6\n");
    EXPECT_EQ(s, tail(3));

    VLOG_IF(true, 3) << "Test conditional verbose log";
    s = BUILD_STR(getDate() << " VERBOSE-3 Test conditional verbose log\n");
    EXPECT_EQ(s, tail(1));

    VLOG_IF(false, 3) << "SHOULD NOT LOG";
    // Should not log!
    EXPECT_EQ(s, tail(1));

    VLOG(3) << "Log normally (verbose)";
    s = BUILD_STR(getDate() << " VERBOSE-3 Log normally (verbose)\n");
    EXPECT_EQ(s, tail(1));

    // Reset it back to old
    Loggers::reconfigureAllLoggers(cOld);
}

TEST(WriteAllTest, EVERY_N) {
    std::string s;
    const char* levelName = "INFO";
    for (int i = 1; i <= 6; ++i)
        LOG_EVERY_N(2, INFO) << levelName << " every n=" << i;

    s = BUILD_STR(getDate() << " " << levelName << " every n=2\n"
               << getDate() << " " << levelName << " every n=4\n"
               << getDate() << " " << levelName << " every n=6\n");
    EXPECT_EQ(s, tail(3));
}

TEST(WriteAllTest, AFTER_N) {
    std::string s;
    const char* levelName = "INFO";
    for (int i = 1; i <= 6; ++i)
        LOG_AFTER_N(2, INFO) << levelName << " after n=" << i;

    s = BUILD_STR(getDate() << " " << levelName << " after n=3\n"
               << getDate() << " " << levelName << " after n=4\n"
               << getDate() << " " << levelName << " after n=5\n"
               << getDate() << " " << levelName << " after n=6\n");
    EXPECT_EQ(s, tail(4));
}

TEST(WriteAllTest, N_TIMES) {
    std::string s;
    const char* levelName = "INFO";
    for (int i = 1; i <= 6; ++i)
        LOG_N_TIMES(4, INFO) << levelName << " n times=" << i;

    s = BUILD_STR(getDate() << " " << levelName << " n times=1\n"
               << getDate() << " " << levelName << " n times=2\n"
               << getDate() << " " << levelName << " n times=3\n"
               << getDate() << " " << levelName << " n times=4\n");
    EXPECT_EQ(s, tail(4));
}
#endif // WRITE_ALL_TEST_H_
