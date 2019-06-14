/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#pragma once

#include "utils/Error.h"
#include "server/ServerConfig.h"
#include "SystemInfo.h"

namespace zilliz {
namespace milvus {
namespace server {
class MetricsBase{
 public:
    static MetricsBase&
    GetInstance(){
        static MetricsBase instance;
        return instance;
    }

    virtual ServerError Init() {};
    virtual void AddGroupSuccessTotalIncrement(double value = 1) {};
    virtual void AddGroupFailTotalIncrement(double value = 1) {};
    virtual void HasGroupSuccessTotalIncrement(double value = 1) {};
    virtual void HasGroupFailTotalIncrement(double value = 1) {};
    virtual void GetGroupSuccessTotalIncrement(double value = 1) {};
    virtual void GetGroupFailTotalIncrement(double value = 1) {};
    virtual void GetGroupFilesSuccessTotalIncrement(double value = 1) {};
    virtual void GetGroupFilesFailTotalIncrement(double value = 1) {};
    virtual void AddVectorsSuccessTotalIncrement(double value = 1) {};
    virtual void AddVectorsFailTotalIncrement(double value = 1) {};
    virtual void AddVectorsDurationHistogramOberve(double value) {};
    virtual void SearchSuccessTotalIncrement(double value = 1) {};
    virtual void SearchFailTotalIncrement(double value = 1) {};
    virtual void SearchDurationHistogramObserve(double value) {};
    virtual void RawFileSizeHistogramObserve(double value) {};
    virtual void IndexFileSizeHistogramObserve(double value) {};
    virtual void BuildIndexDurationSecondsHistogramObserve(double value) {};
    virtual void AllBuildIndexDurationSecondsHistogramObserve(double value) {};
    virtual void CacheUsageGaugeIncrement(double value = 1) {};
    virtual void CacheUsageGaugeDecrement(double value = 1) {};
    virtual void CacheUsageGaugeSet(double value) {};
    virtual void MetaVisitTotalIncrement(double value = 1) {};
    virtual void MetaVisitDurationSecondsHistogramObserve(double value) {};
    virtual void MemUsagePercentGaugeSet(double value) {};
    virtual void MemUsagePercentGaugeIncrement(double value = 1) {};
    virtual void MemUsagePercentGaugeDecrement(double value = 1) {};
    virtual void MemUsageTotalGaugeSet(double value) {};
    virtual void MemUsageTotalGaugeIncrement(double value = 1) {};
    virtual void MemUsageTotalGaugeDecrement(double value = 1) {};
    virtual void MetaAccessTotalIncrement(double value = 1) {};
    virtual void MetaAccessDurationSecondsHistogramObserve(double value) {};
    virtual void FaissDiskLoadDurationSecondsHistogramObserve(double value) {};
    virtual void FaissDiskLoadSizeBytesHistogramObserve(double value) {};
    virtual void FaissDiskLoadIOSpeedHistogramObserve(double value) {};
    virtual void CacheAccessTotalIncrement(double value = 1) {};
    virtual void MemTableMergeDurationSecondsHistogramObserve(double value) {};
    virtual void SearchIndexDataDurationSecondsHistogramObserve(double value) {};
    virtual void SearchRawDataDurationSecondsHistogramObserve(double value) {};
    virtual void IndexFileSizeTotalIncrement(double value = 1) {};
    virtual void RawFileSizeTotalIncrement(double value = 1) {};
    virtual void IndexFileSizeGaugeSet(double value) {};
    virtual void RawFileSizeGaugeSet(double value) {};
    virtual void FaissDiskLoadIOSpeedGaugeSet(double value) {};
    virtual void QueryResponseSummaryObserve(double value) {};
    virtual void DiskStoreIOSpeedGaugeSet(double value) {};
    virtual void DataFileSizeGaugeSet(double value) {};
    virtual void AddVectorsSuccessGaugeSet(double value) {};
    virtual void AddVectorsFailGaugeSet(double value) {};
    virtual void QueryVectorResponseSummaryObserve(double value, int count = 1) {};
    virtual void QueryVectorResponsePerSecondGaugeSet(double value) {};
    virtual void CPUUsagePercentSet() {};
    virtual void RAMUsagePercentSet() {};
    virtual void QueryResponsePerSecondGaugeSet(double value) {};
    virtual void GPUPercentGaugeSet() {};
    virtual void GPUMemoryUsageGaugeSet() {};
    virtual void AddVectorsPerSecondGaugeSet(int num_vector, int dim, double time) {};
    virtual void QueryIndexTypePerSecondSet(std::string type, double value) {};
    virtual void ConnectionGaugeIncrement() {};
    virtual void ConnectionGaugeDecrement() {};
    virtual void KeepingAliveCounterIncrement(double value = 1) {};
    virtual void OctetsSet() {};
};






}
}
}