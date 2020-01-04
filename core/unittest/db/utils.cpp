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

#include "db/utils.h"

#include <opentracing/mocktracer/tracer.h>

#include <boost/filesystem.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "db/DBFactory.h"
#include "db/Options.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif
#include "utils/CommonUtil.h"

INITIALIZE_EASYLOGGINGPP

namespace {

static const char* CONFIG_STR =
    "# All the following configurations are default values.\n"
    "\n"
    "server_config:\n"
    "  address: 0.0.0.0                  # milvus server ip address (IPv4)\n"
    "  port: 19530                       # port range: 1025 ~ 65534\n"
    "  deploy_mode: single               \n"
    "  time_zone: UTC+8\n"
    "\n"
    "db_config:\n"
    "  backend_url: sqlite://:@:/        \n"
    "                                    \n"
    "                                    # Replace 'dialect' with 'mysql' or 'sqlite'\n"
    "\n"
    "  insert_buffer_size: 4             # GB, maximum insert buffer size allowed\n"
    "\n"
    "storage_config:\n"
    "  primary_path: /tmp/milvus         # path used to store data and meta\n"
    "  secondary_path:                   # path used to store data only, split by semicolon\n"
    "\n"
    "metric_config:\n"
    "  enable_monitor: false             # enable monitoring or not\n"
    "  collector: prometheus             # prometheus\n"
    "  prometheus_config:\n"
    "    port: 8080                      # port prometheus used to fetch metrics\n"
    "\n"
    "cache_config:\n"
    "  cpu_mem_capacity: 16              # GB, CPU memory used for cache\n"
    "  cpu_mem_threshold: 0.85           # percentage of data kept when cache cleanup triggered\n"
    "  cache_insert_data: false          # whether load inserted data into cache\n"
    "\n"
    "engine_config:\n"
    "  use_blas_threshold: 20\n"
    "\n"
#ifdef MILVUS_GPU_VERSION
    "gpu_resource_config:\n"
    "  enable: true                      # whether to enable GPU resources\n"
    "  cache_capacity: 4                 # GB, size of GPU memory per card used for cache, must be a positive integer\n"
    "  search_resources:                 # define the GPU devices used for search computation, must be in format gpux\n"
    "    - gpu0\n"
    "  build_index_resources:            # define the GPU devices used for index building, must be in format gpux\n"
    "    - gpu0\n"
#endif
    "\n";

void
WriteToFile(const std::string& file_path, const char* content) {
    std::fstream fs(file_path.c_str(), std::ios_base::out);

    // write data to file
    fs << content;
    fs.close();
}

class DBTestEnvironment : public ::testing::Environment {
 public:
    explicit DBTestEnvironment(const std::string& uri) : uri_(uri) {
    }

    std::string
    getURI() const {
        return uri_;
    }

    void
    SetUp() override {
        getURI();
    }

 private:
    std::string uri_;
};

DBTestEnvironment* test_env = nullptr;

}  // namespace

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
BaseTest::InitLog() {
    el::Configurations defaultConf;
    defaultConf.setToDefault();
    defaultConf.set(el::Level::Debug, el::ConfigurationType::Format, "[%thread-%datetime-%level]: %msg (%fbase:%line)");
    el::Loggers::reconfigureLogger("default", defaultConf);
}

void
BaseTest::SetUp() {
    InitLog();
    dummy_context_ = std::make_shared<milvus::server::Context>("dummy_request_id");
    opentracing::mocktracer::MockTracerOptions tracer_options;
    auto mock_tracer =
        std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
    auto mock_span = mock_tracer->StartSpan("mock_span");
    auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
    dummy_context_->SetTraceContext(trace_context);
#ifdef MILVUS_GPU_VERSION
    knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(0, 1024 * 1024 * 200, 1024 * 1024 * 300, 2);
#endif
}

void
BaseTest::TearDown() {
    milvus::cache::CpuCacheMgr::GetInstance()->ClearCache();
#ifdef MILVUS_GPU_VERSION
    milvus::cache::GpuCacheMgr::GetInstance(0)->ClearCache();
    knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
}

milvus::engine::DBOptions
BaseTest::GetOptions() {
    auto options = milvus::engine::DBFactory::BuildOption();
    options.meta_.path_ = CONFIG_PATH;
    options.meta_.backend_uri_ = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
DBTest::SetUp() {
    BaseTest::SetUp();

    auto res_mgr = milvus::scheduler::ResMgrInst::GetInstance();
    res_mgr->Clear();
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("disk", "DISK", 0, false));
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("cpu", "CPU", 0));

    auto default_conn = milvus::scheduler::Connection("IO", 500.0);
    auto PCIE = milvus::scheduler::Connection("IO", 11000.0);
    res_mgr->Connect("disk", "cpu", default_conn);
#ifdef MILVUS_GPU_VERSION
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("0", "GPU", 0));
    res_mgr->Connect("cpu", "0", PCIE);
#endif
    res_mgr->Start();
    milvus::scheduler::SchedInst::GetInstance()->Start();

    milvus::scheduler::JobMgrInst::GetInstance()->Start();

    auto options = GetOptions();
    db_ = milvus::engine::DBFactory::Build(options);

    std::string config_path(options.meta_.path_ + CONFIG_FILE);
    WriteToFile(config_path, CONFIG_STR);
}

void
DBTest::TearDown() {
    db_->Stop();
    db_->DropAll();

    milvus::scheduler::JobMgrInst::GetInstance()->Stop();
    milvus::scheduler::SchedInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Clear();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
milvus::engine::DBOptions
DBTest2::GetOptions() {
    auto options = milvus::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.archive_conf_ = milvus::engine::ArchiveConf("delete", "disk:1");
    options.meta_.backend_uri_ = "sqlite://:@:/";
    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
MetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<milvus::engine::meta::SqliteMetaImpl>(options.meta_);
}

void
MetaTest::TearDown() {
    impl_->DropAll();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
milvus::engine::DBOptions
MySqlDBTest::GetOptions() {
    auto options = milvus::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = test_env->getURI();

    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
MySqlMetaTest::SetUp() {
    BaseTest::SetUp();

    auto options = GetOptions();
    impl_ = std::make_shared<milvus::engine::meta::MySQLMetaImpl>(options.meta_, options.mode_);
}

void
MySqlMetaTest::TearDown() {
    impl_->DropAll();

    BaseTest::TearDown();

    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);
}

milvus::engine::DBOptions
MySqlMetaTest::GetOptions() {
    auto options = milvus::engine::DBFactory::BuildOption();
    options.meta_.path_ = "/tmp/milvus_test";
    options.meta_.backend_uri_ = test_env->getURI();

    return options;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int
main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::string uri;
    if (argc > 1) {
        uri = argv[1];
    }

    test_env = new DBTestEnvironment(uri);
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
