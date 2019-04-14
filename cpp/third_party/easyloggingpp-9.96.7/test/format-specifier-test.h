#ifndef FORMAT_SPECIFIER_TEST_H
#define FORMAT_SPECIFIER_TEST_H

#include "test.h"

TEST(FormatSpecifierTest, TestFBaseSpecifier) {
    Configurations c;
    c.setGlobally(el::ConfigurationType::Format, "%datetime{%a %b %d, %H:%m} %fbase: %msg");
    el::Loggers::reconfigureLogger(consts::kDefaultLoggerId, c);
    LOG(INFO) << "My fbase test";
    std::string s = BUILD_STR(getDate() << " format-specifier-test.h: My fbase test\n");
    EXPECT_EQ(s, tail(1));
    // Reset back
    reconfigureLoggersForTest();
}

TEST(FormatSpecifierTest, TestLevShortSpecifier) {
    const char* param[10];
    param[0] = "myprog";
    param[1] = "--v=5";
    param[2] = "\0";
    el::Helpers::setArgs(2, param);


    // Regression origional %level still correct
    Configurations c;
    c.setGlobally(el::ConfigurationType::Format, "%level %msg");
    c.set(el::Level::Verbose, el::ConfigurationType::Format, "%level-%vlevel %msg");
    el::Loggers::reconfigureLogger(consts::kDefaultLoggerId, c);
    {
        std::string levelINFO  = "INFO hello world\n";
        std::string levelDEBUG = "DEBUG hello world\n";
        std::string levelWARN  = "WARNING hello world\n";
        std::string levelERROR = "ERROR hello world\n";
        std::string levelFATAL = "FATAL hello world\n";
        std::string levelVER   = "VERBOSE-2 hello world\n";
        std::string levelTRACE = "TRACE hello world\n";
        LOG(INFO) << "hello world";
        EXPECT_EQ(levelINFO, tail(1));
        LOG(DEBUG) << "hello world";
        EXPECT_EQ(levelDEBUG, tail(1));
        LOG(WARNING) << "hello world";
        EXPECT_EQ(levelWARN, tail(1));
        LOG(ERROR) << "hello world";
        EXPECT_EQ(levelERROR, tail(1));
        LOG(FATAL) << "hello world";
        EXPECT_EQ(levelFATAL, tail(1));
        VLOG(2) << "hello world";
        EXPECT_EQ(levelVER, tail(1));
        LOG(TRACE) << "hello world";
        EXPECT_EQ(levelTRACE, tail(1));
    }

    // Test %levshort new specifier
    c.setGlobally(el::ConfigurationType::Format, "%levshort  %msg");
    c.set(el::Level::Verbose, el::ConfigurationType::Format, "%levshort%vlevel %msg");
    el::Loggers::reconfigureLogger(consts::kDefaultLoggerId, c);
    {
        std::string levelINFO  = "I  hello world\n";
        std::string levelDEBUG = "D  hello world\n";
        std::string levelWARN  = "W  hello world\n";
        std::string levelERROR = "E  hello world\n";
        std::string levelFATAL = "F  hello world\n";
        std::string levelVER   = "V2 hello world\n";
        std::string levelTRACE = "T  hello world\n";
        LOG(INFO) << "hello world";
        EXPECT_EQ(levelINFO, tail(1));
        LOG(DEBUG) << "hello world";
        EXPECT_EQ(levelDEBUG, tail(1));
        LOG(WARNING) << "hello world";
        EXPECT_EQ(levelWARN, tail(1));
        LOG(ERROR) << "hello world";
        EXPECT_EQ(levelERROR, tail(1));
        LOG(FATAL) << "hello world";
        EXPECT_EQ(levelFATAL, tail(1));
        VLOG(2) << "hello world";
        EXPECT_EQ(levelVER, tail(1));
        LOG(TRACE) << "hello world";
        EXPECT_EQ(levelTRACE, tail(1));
    }

    // Reset back
    reconfigureLoggersForTest();
}

#endif // FORMAT_SPECIFIER_TEST_H
