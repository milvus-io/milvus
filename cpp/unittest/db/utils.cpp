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
#include "server/ServerConfig.h"

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

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void BaseTest::InitLog() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug,
            el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}

void BaseTest::SetUp() {
    InitLog();
}

engine::Options BaseTest::GetOptions() {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.backend_uri = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void DBTest::SetUp() {
    BaseTest::SetUp();

    server::ConfigNode& config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_CACHE);
    config.AddSequenceItem(server::CONFIG_GPU_IDS, "0");

    auto res_mgr = engine::ResMgrInst::GetInstance();
    res_mgr->Clear();
    res_mgr->Add(engine::ResourceFactory::Create("disk", "DISK", 0, true, false));
    res_mgr->Add(engine::ResourceFactory::Create("cpu", "CPU", 0, true, true));
    res_mgr->Add(engine::ResourceFactory::Create("gtx1660", "GPU", 0, true, true));

    auto default_conn = engine::Connection("IO", 500.0);
    auto PCIE = engine::Connection("IO", 11000.0);
    res_mgr->Connect("disk", "cpu", default_conn);
    res_mgr->Connect("cpu", "gtx1660", PCIE);
    res_mgr->Start();
    engine::SchedInst::GetInstance()->Start();

    auto options = GetOptions();
    db_ = engine::DBFactory::Build(options);
}

void DBTest::TearDown() {
    db_->Stop();
    db_->DropAll();
    delete db_;

    engine::ResMgrInst::GetInstance()->Stop();
    engine::SchedInst::GetInstance()->Stop();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta.path);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
engine::Options DBTest2::GetOptions() {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.archive_conf = engine::ArchiveConf("delete", "disk:1");
    options.meta.backend_uri = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void MetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<engine::meta::SqliteMetaImpl>(options.meta);
}

void MetaTest::TearDown() {
    impl_->DropAll();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta.path);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
engine::Options MySqlDBTest::GetOptions() {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.backend_uri = DBTestEnvironment::getURI();

    if(options.meta.backend_uri.empty()) {
        options.meta.backend_uri = "mysql://root:Fantast1c@192.168.1.194:3306/";
    }

    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void MySqlMetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<engine::meta::MySQLMetaImpl>(options.meta, options.mode);
}

void MySqlMetaTest::TearDown() {
    impl_->DropAll();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta.path);
}

zilliz::milvus::engine::Options MySqlMetaTest::GetOptions() {
    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.backend_uri = DBTestEnvironment::getURI();

    if(options.meta.backend_uri.empty()) {
        options.meta.backend_uri = "mysql://root:Fantast1c@192.168.1.194:3306/";
    }

    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (argc > 1) {
        uri = argv[1];
    }

//    if(uri.empty()) {
//        uri = "mysql://root:Fantast1c@192.168.1.194:3306/";
//    }
//    std::cout << uri << std::endl;
    ::testing::AddGlobalTestEnvironment(new DBTestEnvironment);
    return RUN_ALL_TESTS();
}
