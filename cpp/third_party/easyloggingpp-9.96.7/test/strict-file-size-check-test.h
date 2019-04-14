#ifndef STRICT_FILE_SIZE_CHECK_TEST_H
#define STRICT_FILE_SIZE_CHECK_TEST_H

#include "test.h"

static bool handlerCalled;

void handler(const char*, std::size_t) {
    handlerCalled = true;
}

TEST(StrictFileSizeCheckTest, HandlerCalled) {
    EXPECT_FALSE(handlerCalled);
    EXPECT_TRUE(ELPP->hasFlag(LoggingFlag::StrictLogFileSizeCheck));

    el::Loggers::getLogger("handler_check_logger");
    el::Loggers::reconfigureLogger("handler_check_logger", el::ConfigurationType::Filename, "/tmp/logs/max-size.log");
    el::Loggers::reconfigureLogger("handler_check_logger", el::ConfigurationType::MaxLogFileSize, "100");
    el::Helpers::installPreRollOutCallback(handler);
    for (int i = 0; i < 100; ++i) {
        CLOG(INFO, "handler_check_logger") << "Test " << i;
    }
    EXPECT_TRUE(handlerCalled);
}

#endif // STRICTFILESIZECHECKTEST_H
