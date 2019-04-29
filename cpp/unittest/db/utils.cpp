////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <iostream>
#include <easylogging++.h>

#include "utils.h"

using namespace zilliz::vecwise;

void ASSERT_STATS(engine::Status& stat) {
    ASSERT_TRUE(stat.ok());
    if(!stat.ok()) {
        std::cout << stat.ToString() << std::endl;
    }
}

void DBTest::SetUp() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug,
            el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}
