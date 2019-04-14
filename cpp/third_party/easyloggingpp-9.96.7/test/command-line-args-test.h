#ifndef COMMAND_LINE_ARGS_TEST_H
#define COMMAND_LINE_ARGS_TEST_H

#include "test.h"

TEST(CommandLineArgsTest, SetArgs) {
    const char* c[10];
    c[0] = "myprog";
    c[1] = "-arg1";
    c[2] = "--arg2WithValue=1";
    c[3] = "--arg3WithValue=something=some_other_thing";
    c[4] = "-arg1"; // Shouldn't be added
    c[5] = "--arg3WithValue=this_should_be_ignored_since_key_already_exist";
    c[6] = "--arg4WithValue=this_should_Added";
    c[7] = "\0";
    CommandLineArgs cmd(7, c);
    EXPECT_EQ(4, cmd.size());
    EXPECT_FALSE(cmd.hasParamWithValue("-arg1"));
    EXPECT_FALSE(cmd.hasParam("--arg2WithValue"));
    EXPECT_FALSE(cmd.hasParam("--arg3WithValue"));
    EXPECT_TRUE(cmd.hasParamWithValue("--arg2WithValue"));
    EXPECT_TRUE(cmd.hasParamWithValue("--arg3WithValue"));
    EXPECT_TRUE(cmd.hasParam("-arg1"));
    EXPECT_STRCASEEQ(cmd.getParamValue("--arg2WithValue"), "1");
    EXPECT_STRCASEEQ(cmd.getParamValue("--arg3WithValue"), "something=some_other_thing");
    EXPECT_STRCASEEQ(cmd.getParamValue("--arg4WithValue"), "this_should_Added");
}

TEST(CommandLineArgsTest, LoggingFlagsArg) {
    const char* c[3];
    c[0] = "myprog";
    c[1] = "--logging-flags=5"; // NewLineForContainer & LogDetailedCrashReason (1 & 4)
    c[2] = "\0";

    unsigned short currFlags = ELPP->flags(); // For resetting after test
	
    EXPECT_FALSE(Loggers::hasFlag(LoggingFlag::NewLineForContainer));
    EXPECT_FALSE(Loggers::hasFlag(LoggingFlag::LogDetailedCrashReason));

    Helpers::setArgs(2, c);

    EXPECT_TRUE(Loggers::hasFlag(LoggingFlag::NewLineForContainer));
    EXPECT_TRUE(Loggers::hasFlag(LoggingFlag::LogDetailedCrashReason));

    // Reset to original state
    std::stringstream resetter;
    resetter << "--logging-flags=" << currFlags;
    c[1] = resetter.str().c_str();
    Helpers::setArgs(2, c);
    EXPECT_FALSE(Loggers::hasFlag(LoggingFlag::NewLineForContainer));
    EXPECT_FALSE(Loggers::hasFlag(LoggingFlag::LogDetailedCrashReason));

}

#endif // COMMAND_LINE_ARGS_TEST_H
