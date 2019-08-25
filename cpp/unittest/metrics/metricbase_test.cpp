/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "metrics/Metrics.h"

#include <gtest/gtest.h>
#include <iostream>

using namespace zilliz::milvus;

TEST(MetricbaseTest, METRICBASE_TEST){
    server::MetricsBase instance = server::MetricsBase::GetInstance();
    instance.Init();
    server::SystemInfo::GetInstance().Init();
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
    instance.AddVectorsPerSecondGaugeSet(1,1,1);
    instance.QueryIndexTypePerSecondSet("IVF", 1.0);
    instance.ConnectionGaugeIncrement();
    instance.ConnectionGaugeDecrement();
    instance.KeepingAliveCounterIncrement();
    instance.OctetsSet();
}