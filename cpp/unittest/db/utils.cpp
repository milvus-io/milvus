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
#include <memory>
#include <string>
#include <boost/filesystem.hpp>

#include "db/utils.h"
#include "cache/GpuCacheMgr.h"
#include "cache/CpuCacheMgr.h"
#include "db/DBFactory.h"
#include "db/Options.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"

INITIALIZE_EASYLOGGINGPP

namespace {

namespace ms = milvus;

class DBTestEnvironment : public ::testing::Environment {
 public:
    explicit DBTestEnvironment(const std::string& uri)
        : uri_(uri) {
    }

    std::string getURI() const {
        return uri_;
    }

    void SetUp() override {
        getURI();
    }

 private:
    std::string uri_;
};

DBTestEnvironment* test_env = nullptr;

} // namespace



/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
BaseTest::InitLog() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug,
                    el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}

void
BaseTest::SetUp() {
    InitLog();

    knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(0, 1024 * 1024 * 200, 1024 * 1024 * 300, 2);
}

void
BaseTest::TearDown() {
    milvus::cache::CpuCacheMgr::GetInstance()->ClearCache();
    milvus::cache::GpuCacheMgr::GetInstance(0)->ClearCache();
    knowhere::FaissGpuResourceMgr::GetInstance().Free();
}

ms::engine::DBOptions
BaseTest::GetOptions() {
    auto options = ms::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
DBTest::SetUp() {
    BaseTest::SetUp();

    auto res_mgr = ms::scheduler::ResMgrInst::GetInstance();
    res_mgr->Clear();
    res_mgr->Add(ms::scheduler::ResourceFactory::Create("disk", "DISK", 0, true, false));
    res_mgr->Add(ms::scheduler::ResourceFactory::Create("cpu", "CPU", 0, true, false));
    res_mgr->Add(ms::scheduler::ResourceFactory::Create("gtx1660", "GPU", 0, true, true));

    auto default_conn = ms::scheduler::Connection("IO", 500.0);
    auto PCIE = ms::scheduler::Connection("IO", 11000.0);
    res_mgr->Connect("disk", "cpu", default_conn);
    res_mgr->Connect("cpu", "gtx1660", PCIE);
    res_mgr->Start();
    ms::scheduler::SchedInst::GetInstance()->Start();

    ms::scheduler::JobMgrInst::GetInstance()->Start();

    auto options = GetOptions();
    db_ = ms::engine::DBFactory::Build(options);
}

void
DBTest::TearDown() {
    db_->Stop();
    db_->DropAll();

    ms::scheduler::JobMgrInst::GetInstance()->Stop();
    ms::scheduler::SchedInst::GetInstance()->Stop();
    ms::scheduler::ResMgrInst::GetInstance()->Stop();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ms::engine::DBOptions
DBTest2::GetOptions() {
    auto options = ms::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.archive_conf_ = ms::engine::ArchiveConf("delete", "disk:1");
    options.meta_.backend_uri_ = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
MetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<ms::engine::meta::SqliteMetaImpl>(options.meta_);
}

void
MetaTest::TearDown() {
    impl_->DropAll();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ms::engine::DBOptions
MySqlDBTest::GetOptions() {
    auto options = ms::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = test_env->getURI();

    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
MySqlMetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<ms::engine::meta::MySQLMetaImpl>(options.meta_, options.mode_);
}

void
MySqlMetaTest::TearDown() {
    impl_->DropAll();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

ms::engine::DBOptions
MySqlMetaTest::GetOptions() {
    auto options = ms::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = test_env->getURI();

    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int
main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::string uri;
    if (argc > 1) {
        uri = argv[1];
    }

    test_env = new DBTestEnvironment(uri);
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
