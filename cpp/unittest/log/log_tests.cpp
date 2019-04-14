////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "easylogging++.h"

class LogTest: public testing::Test {
protected:
    void SetUp() override {
        el::Configurations conf("../../../conf/vecwise_engine_log.conf");
        el::Loggers::reconfigureAllLoggers(conf);
    }
};

TEST_F(LogTest, TEST) {
    EXPECT_FALSE(el::Loggers::hasFlag(el::LoggingFlag::NewLineForContainer));
    EXPECT_FALSE(el::Loggers::hasFlag(el::LoggingFlag::LogDetailedCrashReason));
}