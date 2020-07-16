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

#include "ssdb/utils.h"

#include <opentracing/mocktracer/tracer.h>

#include <boost/filesystem.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <fiu-local.h>
#include <random>

#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "db/DBFactory.h"
#include "db/Options.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Snapshots.h"
#include "db/snapshot/ResourceHolders.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

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
    "  meta_uri: sqlite://:@:/\n"
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
BaseTest::SetUp() {
    InitLog();
}

void
BaseTest::TearDown() {
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SnapshotTest::SetUp() {
    BaseTest::SetUp();
    milvus::engine::snapshot::Store::GetInstance().Mock();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Start();
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

    milvus::engine::snapshot::Snapshots::GetInstance().Reset();

    milvus::engine::snapshot::Snapshots::GetInstance().Init();
}

void
SnapshotTest::TearDown() {
    // TODO: Temp to delay some time. OperationExecutor should wait all resources be destructed before stop
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    milvus::engine::snapshot::EventExecutor::GetInstance().Stop();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Stop();
    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SSDBTest::SetUp() {
    BaseTest::SetUp();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Start();
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
    milvus::engine::snapshot::Store::GetInstance().DoReset();
    milvus::engine::snapshot::Snapshots::GetInstance().Reset();
    milvus::engine::snapshot::Snapshots::GetInstance().Init();

    auto options = milvus::engine::DBOptions();
    options.meta_.path_ = "/tmp/milvus";
    options.meta_.backend_uri_ = "sqlite://:@:/";
    options.wal_enable_ = false;
    db_ = std::make_shared<milvus::engine::SSDBImpl>(options);
}

void
SSDBTest::TearDown() {
    db_ = nullptr;
    // TODO: Temp to delay some time. OperationExecutor should wait all resources be destructed before stop
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    milvus::engine::snapshot::EventExecutor::GetInstance().Stop();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Stop();

    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SSSegmentTest::SetUp() {
    BaseTest::SetUp();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Start();
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

    milvus::engine::snapshot::Store::GetInstance().DoReset();
    milvus::engine::snapshot::Snapshots::GetInstance().Reset();
    milvus::engine::snapshot::Snapshots::GetInstance().Init();

    auto options = milvus::engine::DBOptions();
    options.wal_enable_ = false;
    db_ = std::make_shared<milvus::engine::SSDBImpl>(options);
}

void
SSSegmentTest::TearDown() {
    db_ = nullptr;
    // TODO: Temp to delay some time. OperationExecutor should wait all resources be destructed before stop
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    milvus::engine::snapshot::EventExecutor::GetInstance().Stop();
    milvus::engine::snapshot::OperationExecutor::GetInstance().Stop();

    BaseTest::TearDown();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void
SSMetaTest::SetUp() {
    meta_.TruncateAll();
}

void
SSMetaTest::TearDown() {
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
