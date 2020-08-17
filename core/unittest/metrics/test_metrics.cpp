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

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <gtest/gtest.h>
#include <fiu/fiu-local.h>
#include <fiu-control.h>

#include "config/ServerConfig.h"
#include "metrics/utils.h"
#include "metrics/Metrics.h"

TEST_F(MetricTest, METRIC_TEST) {
    fiu_init(0);

#ifdef MILVUS_GPU_VERSION
    FIU_ENABLE_FIU("SystemInfo.Init.nvmInit_fail");
    milvus::server::SystemInfo::GetInstance().initialized_ = false;
    milvus::server::SystemInfo::GetInstance().Init();
    fiu_disable("SystemInfo.Init.nvmInit_fail");
    FIU_ENABLE_FIU("SystemInfo.Init.nvm_getDevice_fail");
    milvus::server::SystemInfo::GetInstance().initialized_ = false;
    milvus::server::SystemInfo::GetInstance().Init();
    fiu_disable("SystemInfo.Init.nvm_getDevice_fail");
    milvus::server::SystemInfo::GetInstance().initialized_ = false;
#endif

    milvus::server::SystemInfo::GetInstance().Init();
    milvus::server::Metrics::GetInstance().Init();

    std::string system_info;
    milvus::server::SystemInfo::GetInstance().GetSysInfoJsonStr(system_info);
}

TEST_F(MetricTest, COLLECTOR_METRICS_TEST) {
    auto status = milvus::Status::OK();
    milvus::server::CollectInsertMetrics insert_metrics0(0, status);
    status = milvus::Status(milvus::DB_ERROR, "error");
    milvus::server::CollectInsertMetrics insert_metrics1(0, status);

    milvus::server::CollectQueryMetrics query_metrics(10);

    milvus::server::CollectMergeFilesMetrics merge_metrics();

    milvus::server::CollectBuildIndexMetrics build_index_metrics();

    milvus::server::CollectExecutionEngineMetrics execution_metrics(10);

    milvus::server::CollectSerializeMetrics serialize_metrics(10);

    milvus::server::CollectAddMetrics add_metrics(10, 128);

    milvus::server::MetricCollector metric_collector();
}


