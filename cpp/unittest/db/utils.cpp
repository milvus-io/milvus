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
#include "db/Options.h"

using namespace zilliz::milvus;

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

engine::Options DBTest::GetOptions() {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    return options;
}

void DBTest::SetUp() {
    InitLog();
    auto options = GetOptions();
    db_ = engine::DBFactory::Build(options);
}

void DBTest::TearDown() {
    delete db_;
    boost::filesystem::remove_all("/tmp/milvus_test");
}

engine::Options DBTest2::GetOptions() {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.archive_conf = engine::ArchiveConf("delete", "disk:1");
    return options;
}

void MetaTest::SetUp() {
    InitLog();
    impl_ = engine::DBMetaImplFactory::Build();
}

void MetaTest::TearDown() {
    impl_->DropAll();
}

zilliz::milvus::engine::DBMetaOptions MySQLTest::getDBMetaOptions() {
    engine::DBMetaOptions options;
    options.backend_uri = "mysql://root:1234@:/test";
    options.path = "/tmp/milvus_test";
    return options;

}
