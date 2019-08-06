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

    virtual void AddVectorsSuccessTotalIncrement(double value = 1) {};
    virtual void AddVectorsFailTotalIncrement(double value = 1) {};
    virtual void AddVectorsDurationHistogramOberve(double value) {};

    virtual void RawFileSizeHistogramObserve(double value) {};
    virtual void IndexFileSizeHistogramObserve(double value) {};
    virtual void BuildIndexDurationSecondsHistogramObserve(double value) {};

    virtual void CacheUsageGaugeSet(double value) {};

    virtual void MetaAccessTotalIncrement(double value = 1) {};
    virtual void MetaAccessDurationSecondsHistogramObserve(double value) {};
    virtual void FaissDiskLoadDurationSecondsHistogramObserve(double value) {};
    virtual void FaissDiskLoadSizeBytesHistogramObserve(double value) {};
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

    virtual void CPUCoreUsagePercentSet() {};
    virtual void GPUTemperature() {};
    virtual void CPUTemperature() {};
};






}
}
}