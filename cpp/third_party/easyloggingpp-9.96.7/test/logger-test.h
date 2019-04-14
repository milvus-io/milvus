
#ifndef LOGGERS_TEST_H_
#define LOGGERS_TEST_H_

#include "test.h"

TEST(LoggerTest, RegisterTenThousandLoggers) {
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::ToStandardOutput, "true");
    TIMED_SCOPE(timer, "RegisterTenThousandLoggers");
    for (unsigned int i = 1; i <= 10000; ++i) {
        std::stringstream ss;
        ss << "logger" << i;
        Loggers::getLogger(ss.str());
        if (i % 1000 == 0) {
            ss.str("");
            ss << "Registered [" << i << "] loggers";
            PERFORMANCE_CHECKPOINT_WITH_ID(timer, ss.str().c_str());
        }
    }
    PERFORMANCE_CHECKPOINT_WITH_ID(timer, "10,000 loggers registered");
    CLOG(INFO, "logger8478") << "Writing using logger 'logger8478'";
    PERFORMANCE_CHECKPOINT_WITH_ID(timer, "Log written using logger8478");
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::ToStandardOutput, "false");
    // streams should not be more than 10 (this is worse case, otherwise until this point we dont have
    // more than 2 unique loggers registered)
    EXPECT_LT(ELPP->registeredLoggers()->logStreamsReference()->size(), 10);
}

TEST(LoggerTest, CheckTenThousandLoggers) {

    TIMED_SCOPE(timer, "CheckTenThousandLoggers");

    el::base::type::EnumType lIndex = LevelHelper::kMinValid;
    el::base::type::fstream_t* logger1Stream = el::Loggers::getLogger("logger1")->typedConfigurations()->fileStream(LevelHelper::castFromInt(lIndex));
    // Make sure all loggers for all levels have same file stream pointee
    for (unsigned int i = 1; i <= 10000; ++i) {
        std::stringstream ss;
        ss << "logger" << i;
        LevelHelper::forEachLevel(&lIndex, [&]() -> bool {
            EXPECT_EQ(logger1Stream, el::Loggers::getLogger(ss.str())->typedConfigurations()->fileStream(LevelHelper::castFromInt(lIndex)));
            return false;
        });
        lIndex = LevelHelper::kMinValid;
        LOG_EVERY_N(1000, INFO) << "Checked " << i << " loggers";
    }
}

TEST(LoggerTest, ValidId) {
    EXPECT_TRUE(Logger::isValidId("a-valid-id"));
    EXPECT_FALSE(Logger::isValidId("a valid id"));
    EXPECT_FALSE(Logger::isValidId("logger-with-space-at-end "));
    EXPECT_TRUE(Logger::isValidId("logger_with_no_space_at_end"));
    EXPECT_TRUE(Logger::isValidId("my-great-logger-with-number-1055"));
}

#endif // LOGGERS_TEST_H_
