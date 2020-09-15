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

#include <experimental/filesystem>
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
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Snapshots.h"
#include "db/snapshot/ResourceHolders.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif
#include "db/meta/backend/MockEngine.h"
#include "db/meta/backend/MySqlEngine.h"
#include "db/meta/backend/SqliteEngine.h"
#include "db/wal/WalProxy.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"
#include "utils/CommonUtil.h"


INITIALIZE_EASYLOGGINGPP

namespace {

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
BaseTest::SnapshotStart(bool mock_store, DBOptions options) {
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
    DBOptions options;
    options.meta_.path_ = "/tmp/milvus_ss/db";
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
DBOptions
DBTest::GetOptions() {
    milvus::cache::CpuCacheMgr::GetInstance().SetCapacity(256 * milvus::engine::MB);

    auto options = DBOptions();
    options.meta_.path_ = "/tmp/milvus_ss/db";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_enable_ = false;
    options.auto_flush_interval_ = 1;
    return options;
}

void
DBTest::SetUp() {
    BaseTest::SetUp();
    auto options = GetOptions();
    std::experimental::filesystem::create_directories(options.meta_.path_);
    BaseTest::SnapshotStart(false, options);

    dummy_context_ = std::make_shared<milvus::server::Context>("dummy_request_id");
    opentracing::mocktracer::MockTracerOptions tracer_options;
    auto mock_tracer =
        std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
    auto mock_span = mock_tracer->StartSpan("mock_span");
    auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
    dummy_context_->SetTraceContext(trace_context);

    auto res_mgr = milvus::scheduler::ResMgrInst::GetInstance();
    res_mgr->Clear();
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("disk", "DISK", 0, false));
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("cpu", "CPU", 0));

    auto default_conn = milvus::scheduler::Connection("IO", 500.0);
    res_mgr->Connect("disk", "cpu", default_conn);
#ifdef MILVUS_GPU_VERSION
    auto PCIE = milvus::scheduler::Connection("IO", 11000.0);
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("0", "GPU", 0));
    res_mgr->Connect("cpu", "0", PCIE);
#endif
    res_mgr->Start();
    milvus::scheduler::SchedInst::GetInstance()->Start();
    milvus::scheduler::JobMgrInst::GetInstance()->Start();
    milvus::scheduler::CPUBuilderInst::GetInstance()->Start();

    db_ = milvus::engine::DBFactory::BuildDB(GetOptions());
    db_->Start();
}

void
DBTest::TearDown() {
    db_->Stop();
    db_ = nullptr; // db must be stopped before JobMgr and Snapshot

    //TODO: Clear GPU Cache if needed
    milvus::cache::CpuCacheMgr::GetInstance().ClearCache();

    milvus::scheduler::JobMgrInst::GetInstance()->Stop();
    milvus::scheduler::SchedInst::GetInstance()->Stop();
    milvus::scheduler::CPUBuilderInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Clear();

    BaseTest::SnapshotStop();
    auto options = GetOptions();
    /* boost::filesystem::remove_all(options.meta_.path_); */
    std::experimental::filesystem::remove_all(options.meta_.path_);

    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SegmentTest::SetUp() {
    BaseTest::SetUp();
    DBOptions options;
    options.meta_.path_ = "/tmp/milvus_ss/db";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_enable_ = false;
    BaseTest::SnapshotStart(false, options);

    db_ = milvus::engine::DBFactory::BuildDB(options);
    db_->Start();
}

void
SegmentTest::TearDown() {
    db_->Stop();
    db_ = nullptr;
    BaseTest::SnapshotStop();
    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
MetaTest::SetUp() {
    auto engine = std::make_shared<milvus::engine::meta::MockEngine>();
//    milvus::engine::DBMetaOptions options;
//    options.backend_uri_ = "mysql://root:12345678@127.0.0.1:3309/milvus";
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
    DBOptions options;
    options.meta_.path_ = "/tmp/milvus_ss/db";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_enable_ = false;
    BaseTest::SnapshotStart(true, options);
    db_ = milvus::engine::DBFactory::BuildDB(options);
    db_->Start();

    auto res_mgr = milvus::scheduler::ResMgrInst::GetInstance();
    res_mgr->Clear();
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("disk", "DISK", 0, false));
    res_mgr->Add(milvus::scheduler::ResourceFactory::Create("cpu", "CPU", 0));

    auto default_conn = milvus::scheduler::Connection("IO", 500.0);
    res_mgr->Connect("disk", "cpu", default_conn);
#ifdef MILVUS_GPU_VERSION
    auto PCIE = milvus::scheduler::Connection("IO", 11000.0);
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
    db_->Stop();
    db_ = nullptr; // db must be stopped before JobMgr and Snapshot

    milvus::scheduler::JobMgrInst::GetInstance()->Stop();
    milvus::scheduler::SchedInst::GetInstance()->Stop();
    milvus::scheduler::CPUBuilderInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Stop();
    milvus::scheduler::ResMgrInst::GetInstance()->Clear();

    BaseTest::SnapshotStop();
    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
EventTest::SetUp() {
    auto uri = "mock://:@:/";
    store_ = Store::Build(uri, "/tmp/milvus_ss/db");
    store_->DoReset();

    milvus::engine::snapshot::OperationExecutor::Init(store_);
    milvus::engine::snapshot::OperationExecutor::GetInstance().Start();
    milvus::engine::snapshot::EventExecutor::Init(store_);
    milvus::engine::snapshot::EventExecutor::GetInstance().Start();
}

void
EventTest::TearDown() {
    milvus::engine::snapshot::EventExecutor::GetInstance().Stop();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Stop();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DBOptions
WalTest::GetOptions() {
    DBOptions options;
    options.meta_.path_ = "/tmp/milvus_ss/db";
    options.meta_.backend_uri_ = "mock://:@:/";
    options.wal_path_ = "/tmp/milvus_wal";
    options.wal_enable_ = true;
    return options;
}

void
WalTest::SetUp() {
    BaseTest::SetUp();
    auto options = GetOptions();
    std::experimental::filesystem::create_directory(options.wal_path_);
    milvus::engine::DBPtr db = std::make_shared<milvus::engine::DBProxy>(nullptr, GetOptions());
    db_ = std::make_shared<milvus::engine::WalProxy>(db, options);
    db_->Start();
    BaseTest::SnapshotStart(true, options);
}

void
WalTest::TearDown() {
    BaseTest::SnapshotStop();
    db_->Stop();
    db_ = nullptr;
    std::experimental::filesystem::remove_all(GetOptions().wal_path_);
    BaseTest::TearDown();
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
