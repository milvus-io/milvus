////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <iostream>
#include <easylogging++.h>
#include <thread>
#include <boost/filesystem.hpp>

#include "utils.h"
#include "db/Factories.h"

using namespace zilliz::vecwise;

void ASSERT_STATS(engine::Status& stat) {
    ASSERT_TRUE(stat.ok());
    if(!stat.ok()) {
        std::cout << stat.ToString() << std::endl;
    }
}

void DBTest::InitLog() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug,
            el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}

void DBTest::SetUp() {
    InitLog();
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/vecwise_test";
    engine::DB::Open(options, &db_);
}

void DBTest::TearDown() {
    delete db_;
    db_ = nullptr;
    auto options = engine::OptionsFactory::Build();
    boost::filesystem::remove_all("/tmp/vecwise_test");
}

void MetaTest::SetUp() {
    InitLog();
    impl_ = engine::DBMetaImplFactory::Build();
}

void MetaTest::TearDown() {
    impl_->drop_all();
}
