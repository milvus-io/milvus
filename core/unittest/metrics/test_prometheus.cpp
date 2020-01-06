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

#include "server/Config.h"
#include "metrics/prometheus/PrometheusMetrics.h"

#include <gtest/gtest.h>
#include <iostream>

TEST(PrometheusTest, PROMETHEUS_TEST) {
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
    instance.RAMUsagePercentSet();
    instance.QueryResponsePerSecondGaugeSet(1.0);
    instance.GPUPercentGaugeSet();
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
    instance.GPUTemperature();
    instance.CPUTemperature();

    milvus::server::Config::GetInstance().SetMetricConfigEnableMonitor("off");
    instance.Init();
    instance.CPUCoreUsagePercentSet();
    instance.GPUTemperature();
    instance.CPUTemperature();
}
