#ifndef GLOBAL_CONFIGURATIONS_TEST_H_
#define GLOBAL_CONFIGURATIONS_TEST_H_

#include "test.h"

TEST(GlobalConfigurationTest, Parse) {
    const char* globalConfFile = "/tmp/global-conf-test.conf";
    std::fstream confFile(globalConfFile, std::fstream::out);
    confFile
    << "-- performance\n"
    << "    ## This just skips configuring performance logger any more.\n"
    << "-- global-test-logger\n"
    << "* GLOBAL:\n"
    << "    FORMAT               =  GLOBAL_TEST\n"
    << "* INFO:\n"
    // Following should be included in format because its inside the quotes
    << "* DEBUG:\n"
    << "    FORMAT               =  %datetime %level [%user@%host] [%func] [%loc] %msg ## Comment before EOL char\n"
    << "## Comment on empty line\n"
    // WARNING is defined by GLOBAL
    << "* ERROR:\n"
    << "    FORMAT               =  %datetime %level %msg\n"
    << "* FATAL:\n"
    << "    FORMAT               =  %datetime %level %msg\n"
    << "* VERBOSE:\n"
    << "    FORMAT               =  %datetime %level-%vlevel %msg\n"
    << "* TRACE:\n"
    << "    FORMAT               =  %datetime %level [%func] [%loc] %msg\n";
    confFile.close();

    Logger* perfLogger = Loggers::getLogger("performance", false);
    CHECK_NOTNULL(perfLogger);

    std::string perfFormatConf = perfLogger->configurations()->get(Level::Info, ConfigurationType::Format)->value();
    std::string perfFilenameConf = perfLogger->configurations()->get(Level::Info, ConfigurationType::Filename)->value();
    std::size_t totalLoggers = elStorage->registeredLoggers()->size();

    EXPECT_EQ(Loggers::getLogger("global-test-logger", false), nullptr);

    Loggers::configureFromGlobal(globalConfFile);

    EXPECT_EQ(totalLoggers + 1, elStorage->registeredLoggers()->size());
    EXPECT_EQ(perfFormatConf, perfLogger->configurations()->get(Level::Info, ConfigurationType::Format)->value());
    EXPECT_EQ(perfFilenameConf, perfLogger->configurations()->get(Level::Info, ConfigurationType::Filename)->value());

    // Not nullptr anymore
    Logger* testLogger = Loggers::getLogger("global-test-logger", false);
    EXPECT_NE(testLogger, nullptr);

    EXPECT_EQ("GLOBAL_TEST", testLogger->configurations()->get(Level::Info, ConfigurationType::Format)->value());
    EXPECT_EQ("%datetime %level [%user@%host] [%func] [%loc] %msg", testLogger->configurations()->get(Level::Debug, ConfigurationType::Format)->value());

}

#endif // GLOBAL_CONFIGURATIONS_TEST_H_
