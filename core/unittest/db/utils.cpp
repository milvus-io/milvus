// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "db/utils.h"

#include <opentracing/mocktracer/tracer.h>

#include <boost/filesystem.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <fiu-control.h>
#include <random>

#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "config/ServerConfig.h"
#include "codecs/Codec.h"
#include "db/DBFactory.h"
#include "db/Options.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Snapshots.h"
#include "db/snapshot/ResourceHolders.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif
#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"
#include "utils/CommonUtil.h"


INITIALIZE_EASYLOGGINGPP

namespace {

static const char* CONFIG_STR =
    "version: 0.5\n"
    "\n"
    "cluster:\n"
    "  enable: false\n"
    "  role: rw\n"
    "\n"
    "general:\n"
    "  timezone: UTC+8\n"
    "  meta_uri: mock://:@:/\n"
    "\n"
    "network:\n"
    "  bind.address: 0.0.0.0\n"
    "  bind.port: 19530\n"
    "  http.enable: true\n"
    "  http.port: 19121\n"
    "\n"
    "storage:\n"
    "  path: /tmp/milvus\n"
    "  auto_flush_interval: 1\n"
    "\n"
    "wal:\n"
    "  enable: true\n"
    "  recovery_error_ignore: false\n"
    "  buffer_size: 256MB\n"
    "  path: /tmp/milvus/wal\n"
    "\n"
    "cache:\n"
    "  cache_size: 4GB\n"
    "  insert_buffer_size: 1GB\n"
    "  preload_collection:\n"
    "\n"
    "gpu:\n"
    "  enable: true\n"
    "  cache_size: 1GB\n"
    "  gpu_search_threshold: 1000\n"
    "  search_devices:\n"
    "    - gpu0\n"
    "  build_index_devices:\n"
    "    - gpu0\n"
    "\n"
    "logs:\n"
    "  level: debug\n"
    "  trace.enable: true\n"
    "  path: /tmp/milvus/logs\n"
    "  max_log_file_size: 1024MB\n"
    "  log_rotate_num: 0\n"
    "\n"
    "metric:\n"
    "  enable: false\n"
    "  address: 127.0.0.1\n"
    "  port: 9091\n"
    "\n";

void
WriteToFile(const std::string &file_path, const char *content) {
    std::fstream fs(file_path.c_str(), std::ios_base::out);

    // write data to file
    fs << content;
    fs.close();
}

class DBTestEnvironment : public ::testing::Environment {
 public:
    explicit DBTestEnvironment(const std::string &uri) : uri_(uri) {
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

DBTestEnvironment *test_env = nullptr;

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
BaseTest::SnapshotStart(bool mock_store, milvus::engine::DBOptions options) {
    auto store = Store::Build(options.meta_.backend_uri_, options.meta_.path_,
            milvus::codec::Codec::instance().GetSuffixSet());

    milvus::engine::snapshot::OperationExecutor::Init(store);
    milvus::engine::snapshot::OperationExecutor::GetInstance().Start();
    milvus::engine::snapshot::EventExecutor::Init(store);
    milvus::engine::snapshot::EventExecutor::GetInstance().Start();

    milvus::engine::snapshot::CollectionCommitsHolder::GetInstance().Reset();
    milvus::engine::snapshot::CollectionsHolder::GetInstance().Reset();
    milvus::engine::snapshot::SchemaCommitsHolder::GetInstance().Reset();
    milvus::engine::snapshot::FieldCommitsHolder::GetInstance().Reset();
    milvus::engine::snapshot::FieldsHolder::GetInstance().Reset();
    milvus::engine::snapshot::FieldElementsHolder::GetInstance().Reset();
    milvus::engine::snapshot::PartitionsHolder::GetInstance().Reset();
    milvus::engine::snapshot::PartitionCommitsHolder::GetInstance().Reset();
    milvus::engine::snapshot::SegmentsHolder::GetInstance().Reset();
    milvus::engine::snapshot::SegmentCommitsHolder::GetInstance().Reset();
    milvus::engine::snapshot::SegmentFilesHolder::GetInstance().Reset();

    if (mock_store) {
        store->Mock();
    } else {
        store->DoReset();
    }

    milvus::engine::snapshot::Snapshots::GetInstance().Reset();
    milvus::engine::snapshot::Snapshots::GetInstance().Init(store);
}

void
BaseTest::SnapshotStop() {
    // TODO: Temp to delay some time. OperationExecutor should wait all resources be destructed before stop
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    milvus::engine::snapshot::EventExecutor::GetInstance().Stop();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Stop();
}

void
BaseTest::SetUp() {
    InitLog();
    fiu_init(0);
    fiu_enable_random("Store.ApplyOperation.mock_timeout", 1, nullptr, 0, 0.2);
}

void
BaseTest::TearDown() {
    fiu_disable("Store.ApplyOperation.mock_timeout");
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SnapshotTest::SetUp() {
    BaseTest::SetUp();
    milvus::engine::DBOptions options;
    options.meta_.path_ = "/tmp/milvus_ss";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_enable_ = false;
    BaseTest::SnapshotStart(true, options);
}

void
SnapshotTest::TearDown() {
    BaseTest::SnapshotStop();
    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
milvus::engine::DBOptions
DBTest::GetOptions() {
    milvus::cache::CpuCacheMgr::GetInstance().SetCapacity(256 * milvus::engine::MB);

    auto options = milvus::engine::DBOptions();
    options.meta_.path_ = "/tmp/milvus_ss";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_enable_ = false;
    return options;
}

void
DBTest::SetUp() {
    BaseTest::SetUp();
    BaseTest::SnapshotStart(false, GetOptions());

    dummy_context_ = std::make_shared<milvus::server::Context>("dummy_request_id");
    opentracing::mocktracer::MockTracerOptions tracer_options;
    auto mock_tracer =
        std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
    auto mock_span = mock_tracer->StartSpan("mock_span");
    auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
    dummy_context_->SetTraceContext(trace_context);

    db_ = std::make_shared<milvus::engine::DBImpl>(GetOptions());

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
    milvus::scheduler::CPUBuilderInst::GetInstance()->Start();
}

void
DBTest::TearDown() {
    db_ = nullptr; // db must be stopped before JobMgr and Snapshot

    milvus::scheduler::JobMgrInst::GetInstance()->Stop();
    milvus::scheduler::SchedInst::GetInstance()->Stop();
    milvus::scheduler::CPUBuilderInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Clear();

    BaseTest::SnapshotStop();
    auto options = GetOptions();
    boost::filesystem::remove_all(options.meta_.path_);

    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SegmentTest::SetUp() {
    BaseTest::SetUp();
    milvus::engine::DBOptions options;
    options.meta_.path_ = "/tmp/milvus_ss";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_enable_ = false;
    BaseTest::SnapshotStart(false, options);

    db_ = std::make_shared<milvus::engine::DBImpl>(options);
}

void
SegmentTest::TearDown() {
    BaseTest::SnapshotStop();
    db_ = nullptr;
    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
MetaTest::SetUp() {
    auto engine = std::make_shared<milvus::engine::meta::MockEngine>();
//    milvus::engine::DBMetaOptions options;
//    options.backend_uri_ = "mysql://root:12345678@127.0.0.1:3307/milvus";
//    auto engine = std::make_shared<milvus::engine::meta::MySqlEngine>(options);
    meta_ = std::make_shared<milvus::engine::meta::MetaAdapter>(engine);
    meta_->TruncateAll();
}

void
MetaTest::TearDown() {
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SchedulerTest::SetUp() {
    BaseTest::SetUp();
    milvus::engine::DBOptions options;
    options.meta_.path_ = "/tmp/milvus_ss";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_enable_ = false;
    BaseTest::SnapshotStart(true, options);
    db_ = std::make_shared<milvus::engine::DBImpl>(options);

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
    milvus::scheduler::CPUBuilderInst::GetInstance()->Start();
}

void
SchedulerTest::TearDown() {
    db_ = nullptr; // db must be stopped before JobMgr and Snapshot

    milvus::scheduler::JobMgrInst::GetInstance()->Stop();
    milvus::scheduler::SchedInst::GetInstance()->Stop();
    milvus::scheduler::CPUBuilderInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Clear();

    BaseTest::SnapshotStop();
    BaseTest::TearDown();
}

void
EventTest::SetUp() {
    auto uri = "mock://:@:/";
    store_ = Store::Build(uri, "/tmp/milvus_ss/db");
    store_->DoReset();
}

void
EventTest::TearDown() {
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
