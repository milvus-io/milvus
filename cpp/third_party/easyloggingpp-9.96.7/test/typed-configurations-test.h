#ifndef TYPED_CONFIGURATIONS_TEST_H_
#define TYPED_CONFIGURATIONS_TEST_H_

#include "test.h"
#include <cstdio>

const char* getConfFile(void) {
    const char* file = "/tmp/temp-test.conf";
    std::fstream confFile(file, std::fstream::out);
    confFile << " * GLOBAL:\n"
    << "    FILENAME             =  /tmp/my-test.log\n"
    << "    FORMAT               =  %datetime %level %msg\n"
    << "    MAX_LOG_FILE_SIZE        =  1\n"
    << "    TO_STANDARD_OUTPUT   =  TRUE\n"
    << "* DEBUG:\n"
    // NOTE escaped %level and %host below
    << "    FORMAT               =  %datetime %%level %level [%user@%%host] [%func] [%loc] %msg\n"
    // INFO and WARNING uses is defined by GLOBAL
    << "* ERROR:\n"
    << "    FILENAME             =  /tmp/my-test-err.log\n"
    << "    FORMAT               =  %%logger %%logger %logger %%logger %msg\n"
    << "    MAX_LOG_FILE_SIZE        =  10\n"
    << "* FATAL:\n"
    << "    FORMAT               =  %datetime %%datetime{%H:%m} %level %msg\n"
    << "* VERBOSE:\n"
    << "    FORMAT               =  %%datetime{%h:%m} %datetime %level-%vlevel %msg\n"
    << "* TRACE:\n"
    << "    FORMAT               =  %datetime{%h:%m} %%level %level [%func] [%loc] %msg\n";
    confFile.close();
    return file;
}

TEST(TypedConfigurationsTest, Initialization) {

    std::string testFile = "/tmp/my-test.log";
    std::remove(testFile.c_str());

    Configurations c(getConfFile());
    TypedConfigurations tConf(&c, ELPP->registeredLoggers()->logStreamsReference());

    EXPECT_TRUE(tConf.enabled(Level::Global));

    EXPECT_EQ(ELPP_LITERAL("%datetime %level %msg"), tConf.logFormat(Level::Info).userFormat());
    EXPECT_EQ(ELPP_LITERAL("%datetime INFO %msg"), tConf.logFormat(Level::Info).format());
    EXPECT_EQ("%Y-%M-%d %H:%m:%s,%g", tConf.logFormat(Level::Info).dateTimeFormat());

    EXPECT_EQ(ELPP_LITERAL("%datetime %%level %level [%user@%%host] [%func] [%loc] %msg"), tConf.logFormat(Level::Debug).userFormat());
    std::string expected = BUILD_STR("%datetime %level DEBUG [" << el::base::utils::OS::currentUser() << "@%%host] [%func] [%loc] %msg");
#if defined(ELPP_UNICODE)
    char* orig = Str::wcharPtrToCharPtr(tConf.logFormat(Level::Debug).format().c_str());
#else
    const char* orig = tConf.logFormat(Level::Debug).format().c_str();
#endif
    EXPECT_EQ(expected, std::string(orig));
    EXPECT_EQ("%Y-%M-%d %H:%m:%s,%g", tConf.logFormat(Level::Debug).dateTimeFormat());

    // This double quote is escaped at run-time for %date and %datetime
    EXPECT_EQ(ELPP_LITERAL("%datetime %%datetime{%H:%m} %level %msg"), tConf.logFormat(Level::Fatal).userFormat());
    EXPECT_EQ(ELPP_LITERAL("%datetime %%datetime{%H:%m} FATAL %msg"), tConf.logFormat(Level::Fatal).format());
    EXPECT_EQ("%Y-%M-%d %H:%m:%s,%g", tConf.logFormat(Level::Fatal).dateTimeFormat());

    EXPECT_EQ(ELPP_LITERAL("%datetime{%h:%m} %%level %level [%func] [%loc] %msg"), tConf.logFormat(Level::Trace).userFormat());
    EXPECT_EQ(ELPP_LITERAL("%datetime %level TRACE [%func] [%loc] %msg"), tConf.logFormat(Level::Trace).format());
    EXPECT_EQ("%h:%m", tConf.logFormat(Level::Trace).dateTimeFormat());

    EXPECT_EQ(ELPP_LITERAL("%%datetime{%h:%m} %datetime %level-%vlevel %msg"), tConf.logFormat(Level::Verbose).userFormat());
    EXPECT_EQ(ELPP_LITERAL("%%datetime{%h:%m} %datetime VERBOSE-%vlevel %msg"), tConf.logFormat(Level::Verbose).format());
    EXPECT_EQ("%Y-%M-%d %H:%m:%s,%g", tConf.logFormat(Level::Verbose).dateTimeFormat());

    EXPECT_EQ(ELPP_LITERAL("%%logger %%logger %logger %%logger %msg"), tConf.logFormat(Level::Error).userFormat());
    EXPECT_EQ(ELPP_LITERAL("%%logger %%logger %logger %logger %msg"), tConf.logFormat(Level::Error).format());
    EXPECT_EQ("", tConf.logFormat(Level::Error).dateTimeFormat());
}

TEST(TypedConfigurationsTest, SharedFileStreams) {
    Configurations c(getConfFile());
    TypedConfigurations tConf(&c, ELPP->registeredLoggers()->logStreamsReference());
    // Make sure we have only two unique file streams for ALL and ERROR
    el::base::type::EnumType lIndex = LevelHelper::kMinValid;
    el::base::type::fstream_t* prev = nullptr;
    LevelHelper::forEachLevel(&lIndex, [&]() -> bool {
        if (prev == nullptr) {
            prev = tConf.fileStream(LevelHelper::castFromInt(lIndex));
        } else {
            if (LevelHelper::castFromInt(lIndex) == Level::Error) {
                EXPECT_NE(prev, tConf.fileStream(LevelHelper::castFromInt(lIndex)));
            } else {
                EXPECT_EQ(prev, tConf.fileStream(LevelHelper::castFromInt(lIndex)));
            }
        }
        return false;
    });
}

TEST(TypedConfigurationsTest, NonExistentFileCreation) {
    Configurations c(getConfFile());
    c.setGlobally(ConfigurationType::Filename, "/tmp/logs/el.gtest.log");
    c.set(Level::Info, ConfigurationType::ToFile, "true");
    c.set(Level::Error, ConfigurationType::ToFile, "true");
    c.set(Level::Info, ConfigurationType::Filename, "/a/file/not/possible/to/create/log.log");
    c.set(Level::Error, ConfigurationType::Filename, "/tmp/logs/el.gtest.log");
    TypedConfigurations tConf(&c, ELPP->registeredLoggers()->logStreamsReference());
    EXPECT_TRUE(tConf.toFile(Level::Global));

#if ELPP_OS_EMSCRIPTEN == 1
    // On Emscripten, all files can be created; we actually expect success here
    EXPECT_TRUE(tConf.toFile(Level::Info));
    EXPECT_NE(nullptr, tConf.fileStream(Level::Info));  // not nullptr (emulated fs)
#else
    EXPECT_EQ(nullptr, tConf.fileStream(Level::Info));  // nullptr
    EXPECT_FALSE(tConf.toFile(Level::Info));
#endif

    EXPECT_TRUE(tConf.toFile(Level::Error));
    EXPECT_NE(nullptr, tConf.fileStream(Level::Error)); // Not null
}

TEST(TypedConfigurationsTest, WriteToFiles) {
    std::string testFile = "/tmp/my-test.log";
    Configurations c(getConfFile());
    TypedConfigurations tConf(&c, ELPP->registeredLoggers()->logStreamsReference());
    {
        EXPECT_TRUE(tConf.fileStream(Level::Info)->is_open());
        EXPECT_EQ(testFile, tConf.filename(Level::Info));
        *tConf.fileStream(Level::Info) << "-Info";
        *tConf.fileStream(Level::Debug) << "-Debug";
        tConf.fileStream(Level::Debug)->flush();
        *tConf.fileStream(Level::Error) << "-Error";
        tConf.fileStream(Level::Error)->flush();
    }
    std::ifstream reader(tConf.filename(Level::Info), std::fstream::in);
    std::string line = std::string();
    std::getline(reader, line);
    EXPECT_EQ("-Info-Debug", line);
}

#endif /* TYPED_CONFIGURATIONS_TEST_H_ */
