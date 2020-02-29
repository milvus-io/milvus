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

#include "server/Config.h"
#include "metrics/prometheus/PrometheusMetrics.h"

#include <gtest/gtest.h>
#include <iostream>
#include <fiu-control.h>
#include <fiu-local.h>

TEST(PrometheusTest, PROMETHEUS_TEST) {
    fiu_init(0);
    milvus::server::Config::GetInstance().SetMetricConfigEnableMonitor("on");

    milvus::server::PrometheusMetrics instance = milvus::server::PrometheusMetrics::GetInstance();
    instance.Init();
    instance.SetStartup(true);
    milvus::server::SystemInfo::GetInstance().Init();
    instance.AddVectorsSuccessTotalIncrement();
    instance.AddVectorsFailTotalIncrement();
    instance.AddVectorsDurationHistogramOberve(1.0);
    instance.RawFileSizeHistogramObserve(1.0);
    instance.IndexFileSizeHistogramObserve(1.0);
    instance.BuildIndexDurationSecondsHistogramObserve(1.0);
    instance.CpuCacheUsageGaugeSet(1.0);
    instance.GpuCacheUsageGaugeSet();
    instance.MetaAccessTotalIncrement();
    instance.MetaAccessDurationSecondsHistogramObserve(1.0);
    instance.FaissDiskLoadDurationSecondsHistogramObserve(1.0);
    instance.FaissDiskLoadSizeBytesHistogramObserve(1.0);
    instance.FaissDiskLoadIOSpeedGaugeSet(1.0);
    instance.CacheAccessTotalIncrement();
    instance.MemTableMergeDurationSecondsHistogramObserve(1.0);
    instance.SearchIndexDataDurationSecondsHistogramObserve(1.0);
    instance.SearchRawDataDurationSecondsHistogramObserve(1.0);
    instance.IndexFileSizeTotalIncrement();
    instance.RawFileSizeTotalIncrement();
    instance.IndexFileSizeGaugeSet(1.0);
    instance.RawFileSizeGaugeSet(1.0);
    instance.QueryResponseSummaryObserve(1.0);
    instance.DiskStoreIOSpeedGaugeSet(1.0);
    instance.DataFileSizeGaugeSet(1.0);
    instance.AddVectorsSuccessGaugeSet(1.0);
    instance.AddVectorsFailGaugeSet(1.0);
    instance.QueryVectorResponseSummaryObserve(1.0, 1);
    instance.QueryVectorResponsePerSecondGaugeSet(1.0);
    instance.CPUUsagePercentSet();
    fiu_enable("SystemInfo.CPUPercent.mock", 1, nullptr, 0);
    instance.CPUUsagePercentSet();
    fiu_disable("SystemInfo.CPUPercent.mock");
    instance.RAMUsagePercentSet();
    fiu_enable("SystemInfo.MemoryPercent.mock", 1, nullptr, 0);
    instance.RAMUsagePercentSet();
    fiu_disable("SystemInfo.MemoryPercent.mock");
    instance.QueryResponsePerSecondGaugeSet(1.0);
    instance.GPUPercentGaugeSet();
    fiu_enable("SystemInfo.GPUMemoryTotal.mock", 1, nullptr, 0);
    fiu_enable("SystemInfo.GPUMemoryUsed.mock", 1, nullptr, 0);
    instance.GPUPercentGaugeSet();
    fiu_disable("SystemInfo.GPUMemoryTotal.mock");
    fiu_disable("SystemInfo.GPUMemoryUsed.mock");
    instance.GPUMemoryUsageGaugeSet();
    instance.AddVectorsPerSecondGaugeSet(1, 1, 1);
    instance.QueryIndexTypePerSecondSet("IVF", 1.0);
    instance.QueryIndexTypePerSecondSet("IDMap", 1.0);
    instance.ConnectionGaugeIncrement();
    instance.ConnectionGaugeDecrement();
    instance.KeepingAliveCounterIncrement();
    instance.PushToGateway();
    instance.OctetsSet();

    instance.CPUCoreUsagePercentSet();
    fiu_enable("SystemInfo.getTotalCpuTime.open_proc", 1, nullptr, 0);
    instance.CPUCoreUsagePercentSet();
    fiu_disable("SystemInfo.getTotalCpuTime.open_proc");
    fiu_enable("SystemInfo.getTotalCpuTime.read_proc", 1, nullptr, 0);
    instance.CPUCoreUsagePercentSet();
    fiu_disable("SystemInfo.getTotalCpuTime.read_proc");
    instance.GPUTemperature();
    fiu_enable("SystemInfo.GPUTemperature.mock", 1, nullptr, 0);
    instance.GPUTemperature();
    fiu_disable("SystemInfo.GPUTemperature.mock");
    instance.CPUTemperature();
    fiu_enable("SystemInfo.CPUTemperature.opendir", 1, nullptr, 0);
    instance.CPUTemperature();
    fiu_disable("SystemInfo.CPUTemperature.opendir");
    fiu_enable("SystemInfo.CPUTemperature.openfile", 1, nullptr, 0);
    instance.CPUTemperature();
    fiu_disable("SystemInfo.CPUTemperature.openfile");

    milvus::server::Config::GetInstance().SetMetricConfigEnableMonitor("off");
    instance.Init();
    instance.CPUCoreUsagePercentSet();
    instance.GPUTemperature();
    instance.CPUTemperature();
}
