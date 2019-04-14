#ifndef SYSLOG_TEST_H
#define SYSLOG_TEST_H

#if defined(ELPP_SYSLOG)

#include "test.h"

static const char* kSysLogFile = "/var/log/syslog";
static const char* s_currentHost = el::base::utils::OS::currentHost().c_str();

TEST(SysLogTest, WriteLog) {
    if (!fileExists(kSysLogFile)) {
        // Do not check for syslog config, just dont test it
        return;
    }
    // To avoid "Easylogging++ last message repeated 2 times"
    SYSLOG(INFO) << "last message suppress";

    SYSLOG(INFO) << "this is my syslog";
    sleep(1); // Make sure daemon has picked it up
    std::string expectedEnd = BUILD_STR(s_currentHost << " " << kSysLogIdent << ": INFO : this is my syslog\n");
    std::string actual = tail(1, kSysLogFile);
    EXPECT_TRUE(Str::endsWith(actual, expectedEnd));
}

TEST(SysLogTest, DebugVersionLogs) {
    if (!fileExists(kSysLogFile)) {
        // Do not check for syslog config, just dont test it
        return;
    }
    // Test enabled
    #undef ELPP_DEBUG_LOG
    #define ELPP_DEBUG_LOG 0

    std::string currentTail = tail(1, kSysLogFile);

    DSYSLOG(INFO) << "No DSYSLOG should be resolved";
    sleep(1); // Make sure daemon has picked it up
    EXPECT_TRUE(Str::endsWith(currentTail, tail(1, kSysLogFile)));

    DSYSLOG_IF(true, INFO) << "No DSYSLOG_IF should be resolved";
    sleep(1); // Make sure daemon has picked it up
    EXPECT_TRUE(Str::endsWith(currentTail, tail(1, kSysLogFile)));

    DCSYSLOG(INFO, "performance") << "No DCSYSLOG should be resolved";
    sleep(1); // Make sure daemon has picked it up
    EXPECT_TRUE(Str::endsWith(currentTail, tail(1, kSysLogFile)));

    DCSYSLOG(INFO, "performance") << "No DCSYSLOG should be resolved";
    sleep(1); // Make sure daemon has picked it up
    EXPECT_TRUE(Str::endsWith(currentTail, tail(1, kSysLogFile)));

    // Reset
    #undef ELPP_DEBUG_LOG
    #define ELPP_DEBUG_LOG 1

    // Now test again
    DSYSLOG(INFO) << "DSYSLOG should be resolved";
    sleep(1); // Make sure daemon has picked it up
    std::string expected = BUILD_STR(s_currentHost << " " << kSysLogIdent << ": INFO : DSYSLOG should be resolved\n");
    EXPECT_TRUE(Str::endsWith(tail(1, kSysLogFile), expected));

    DSYSLOG_IF(true, INFO) << "DSYSLOG_IF should be resolved";
    sleep(1); // Make sure daemon has picked it up
    expected = BUILD_STR(s_currentHost << " " << kSysLogIdent << ": INFO : DSYSLOG_IF should be resolved\n");
    EXPECT_TRUE(Str::endsWith(tail(1, kSysLogFile), expected));
    
    DCSYSLOG(INFO, el::base::consts::kSysLogLoggerId) << "DCSYSLOG should be resolved";
    sleep(1); // Make sure daemon has picked it up
    expected = BUILD_STR(s_currentHost << " " << kSysLogIdent << ": INFO : DCSYSLOG should be resolved\n");
    EXPECT_TRUE(Str::endsWith(tail(1, kSysLogFile), expected));

    DCSYSLOG(INFO, el::base::consts::kSysLogLoggerId) << "DCSYSLOG should be resolved";
    sleep(1); // Make sure daemon has picked it up
    expected = BUILD_STR(s_currentHost << " " << kSysLogIdent << ": INFO : DCSYSLOG should be resolved\n");
    EXPECT_TRUE(Str::endsWith(tail(1, kSysLogFile), expected));
}

#else
#   warning "Skipping [SysLogTest] for [ELPP_SYSLOG] not defined"
#endif // defined(ELPP_SYSLOG)

#endif // SYSLOG_TEST_H
