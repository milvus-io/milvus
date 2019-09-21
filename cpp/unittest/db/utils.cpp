// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include <iostream>
#include <thread>
#include <boost/filesystem.hpp>

#include "utils.h"
#include "cache/GpuCacheMgr.h"
#include "cache/CpuCacheMgr.h"
#include "db/DBFactory.h"
#include "db/Options.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"

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

    zilliz::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(0, 1024*1024*200, 1024*1024*300, 2);
}

void BaseTest::TearDown() {
    zilliz::milvus::cache::CpuCacheMgr::GetInstance()->ClearCache();
    zilliz::milvus::cache::GpuCacheMgr::GetInstance(0)->ClearCache();
    zilliz::knowhere::FaissGpuResourceMgr::GetInstance().Free();
}

engine::DBOptions BaseTest::GetOptions() {
    auto options = engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void DBTest::SetUp() {
    BaseTest::SetUp();

    auto res_mgr = scheduler::ResMgrInst::GetInstance();
    res_mgr->Clear();
    res_mgr->Add(scheduler::ResourceFactory::Create("disk", "DISK", 0, true, false));
    res_mgr->Add(scheduler::ResourceFactory::Create("cpu", "CPU", 0, true, false));
    res_mgr->Add(scheduler::ResourceFactory::Create("gtx1660", "GPU", 0, true, true));

    auto default_conn = scheduler::Connection("IO", 500.0);
    auto PCIE = scheduler::Connection("IO", 11000.0);
    res_mgr->Connect("disk", "cpu", default_conn);
    res_mgr->Connect("cpu", "gtx1660", PCIE);
    res_mgr->Start();
    scheduler::SchedInst::GetInstance()->Start();

    scheduler::JobMgrInst::GetInstance()->Start();

    auto options = GetOptions();
    db_ = engine::DBFactory::Build(options);
}

void DBTest::TearDown() {
    db_->Stop();
    db_->DropAll();

    scheduler::JobMgrInst::GetInstance()->Stop();
    scheduler::SchedInst::GetInstance()->Stop();
    scheduler::ResMgrInst::GetInstance()->Stop();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
engine::DBOptions DBTest2::GetOptions() {
    auto options = engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.archive_conf_ = engine::ArchiveConf("delete", "disk:1");
    options.meta_.backend_uri_ = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void MetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<engine::meta::SqliteMetaImpl>(options.meta_);
}

void MetaTest::TearDown() {
    impl_->DropAll();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
engine::DBOptions MySqlDBTest::GetOptions() {
    auto options = engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = DBTestEnvironment::getURI();

    if(options.meta_.backend_uri_.empty()) {
        options.meta_.backend_uri_ = "mysql://root:Fantast1c@192.168.1.194:3306/";
    }

    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void MySqlMetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<engine::meta::MySQLMetaImpl>(options.meta_, options.mode_);
}

void MySqlMetaTest::TearDown() {
    impl_->DropAll();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

engine::DBOptions MySqlMetaTest::GetOptions() {
    auto options = engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = DBTestEnvironment::getURI();

    if(options.meta_.backend_uri_.empty()) {
        options.meta_.backend_uri_ = "mysql://root:Fantast1c@192.168.1.194:3306/";
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
