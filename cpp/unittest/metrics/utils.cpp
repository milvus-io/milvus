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

INITIALIZE_EASYLOGGINGPP

using namespace zilliz::milvus;

static std::string uri;

class DBTestEnvironment : public ::testing::Environment {
public:

//    explicit DBTestEnvironment(std::string uri) : uri_(uri) {}

    static std::string getURI() {
        return uri;
    }

    void SetUp() override {
        getURI();
    }

};

void ASSERT_STATS(engine::Status& stat) {
    ASSERT_TRUE(stat.ok());
    if(!stat.ok()) {
        std::cout << stat.ToString() << std::endl;
    }
}


void MetricTest::InitLog() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug,
            el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}

engine::Options MetricTest::GetOptions() {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.backend_uri = "sqlite://:@:/";
    return options;
}

void MetricTest::SetUp() {
    InitLog();
    auto options = GetOptions();
    db_ = engine::DBFactory::Build(options);
}

void MetricTest::TearDown() {
    delete db_;
    boost::filesystem::remove_all("/tmp/milvus_test");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (argc > 1) {
        uri = argv[1];
    }
//    std::cout << uri << std::endl;
    ::testing::AddGlobalTestEnvironment(new DBTestEnvironment);
    return RUN_ALL_TESTS();
}
