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

#pragma once

#include "MetricBase.h"
#include "db/meta/MetaTypes.h"

namespace milvus {
namespace server {

#define METRICS_NOW_TIME std::chrono::system_clock::now()
#define METRICS_MICROSECONDS(a, b) (std::chrono::duration_cast<std::chrono::microseconds>(b - a)).count();

enum class MetricCollectorType { INVALID, PROMETHEUS, ZABBIX };

class Metrics {
 public:
    static MetricsBase&
    GetInstance();

 private:
    static MetricsBase&
    CreateMetricsCollector();
};
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectMetricsBase {
 protected:
    CollectMetricsBase() {
        start_time_ = METRICS_NOW_TIME;
    }

    virtual ~CollectMetricsBase() = default;

    double
    TimeFromBegine() {
        auto end_time = METRICS_NOW_TIME;
        return METRICS_MICROSECONDS(start_time_, end_time);
    }

 protected:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectInsertMetrics : CollectMetricsBase {
 public:
    CollectInsertMetrics(size_t n, Status& status) : n_(n), status_(status) {
    }

    ~CollectInsertMetrics() {
        if (n_ > 0) {
            auto total_time = TimeFromBegine();
            double avg_time = total_time / n_;
            for (size_t i = 0; i < n_; ++i) {
                Metrics::GetInstance().AddVectorsDurationHistogramOberve(avg_time);
            }

            //    server::Metrics::GetInstance().add_vector_duration_seconds_quantiles().Observe((average_time));
            if (status_.ok()) {
                server::Metrics::GetInstance().AddVectorsSuccessTotalIncrement(n_);
                server::Metrics::GetInstance().AddVectorsSuccessGaugeSet(n_);
            } else {
                server::Metrics::GetInstance().AddVectorsFailTotalIncrement(n_);
                server::Metrics::GetInstance().AddVectorsFailGaugeSet(n_);
            }
        }
    }

 private:
    size_t n_;
    Status& status_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectQueryMetrics : CollectMetricsBase {
 public:
    explicit CollectQueryMetrics(size_t nq) : nq_(nq) {
    }

    ~CollectQueryMetrics() {
        if (nq_ > 0) {
            auto total_time = TimeFromBegine();
            for (size_t i = 0; i < nq_; ++i) {
                server::Metrics::GetInstance().QueryResponseSummaryObserve(total_time);
            }
            auto average_time = total_time / nq_;
            server::Metrics::GetInstance().QueryVectorResponseSummaryObserve(average_time, nq_);
            server::Metrics::GetInstance().QueryVectorResponsePerSecondGaugeSet(double(nq_) / total_time);
        }
    }

 private:
    size_t nq_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectMergeFilesMetrics : CollectMetricsBase {
 public:
    CollectMergeFilesMetrics() {
    }

    ~CollectMergeFilesMetrics() {
        auto total_time = TimeFromBegine();
        server::Metrics::GetInstance().MemTableMergeDurationSecondsHistogramObserve(total_time);
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectBuildIndexMetrics : CollectMetricsBase {
 public:
    CollectBuildIndexMetrics() {
    }

    ~CollectBuildIndexMetrics() {
        auto total_time = TimeFromBegine();
        server::Metrics::GetInstance().BuildIndexDurationSecondsHistogramObserve(total_time);
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectExecutionEngineMetrics : CollectMetricsBase {
 public:
    explicit CollectExecutionEngineMetrics(double physical_size) : physical_size_(physical_size) {
    }

    ~CollectExecutionEngineMetrics() {
        auto total_time = TimeFromBegine();

        server::Metrics::GetInstance().FaissDiskLoadDurationSecondsHistogramObserve(total_time);
        server::Metrics::GetInstance().FaissDiskLoadSizeBytesHistogramObserve(physical_size_);
        server::Metrics::GetInstance().FaissDiskLoadIOSpeedGaugeSet(physical_size_ / double(total_time));
    }

 private:
    double physical_size_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectSerializeMetrics : CollectMetricsBase {
 public:
    explicit CollectSerializeMetrics(size_t size) : size_(size) {
    }

    ~CollectSerializeMetrics() {
        auto total_time = TimeFromBegine();
        server::Metrics::GetInstance().DiskStoreIOSpeedGaugeSet((double)size_ / total_time);
    }

 private:
    size_t size_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectAddMetrics : CollectMetricsBase {
 public:
    CollectAddMetrics(size_t n, uint16_t dimension) : n_(n), dimension_(dimension) {
    }

    ~CollectAddMetrics() {
        auto total_time = TimeFromBegine();
        server::Metrics::GetInstance().AddVectorsPerSecondGaugeSet(static_cast<int>(n_), static_cast<int>(dimension_),
                                                                   total_time);
    }

 private:
    size_t n_;
    uint16_t dimension_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectDurationMetrics : CollectMetricsBase {
 public:
    explicit CollectDurationMetrics(int index_type) : index_type_(index_type) {
    }

    ~CollectDurationMetrics() {
        auto total_time = TimeFromBegine();
        switch (index_type_) {
            case engine::meta::SegmentSchema::RAW: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            case engine::meta::SegmentSchema::TO_INDEX: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            default: {
                server::Metrics::GetInstance().SearchIndexDataDurationSecondsHistogramObserve(total_time);
                break;
            }
        }
    }

 private:
    int index_type_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CollectSearchTaskMetrics : CollectMetricsBase {
 public:
    explicit CollectSearchTaskMetrics(int index_type) : index_type_(index_type) {
    }

    ~CollectSearchTaskMetrics() {
        auto total_time = TimeFromBegine();
        switch (index_type_) {
            case engine::meta::SegmentSchema::RAW: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            case engine::meta::SegmentSchema::TO_INDEX: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            default: {
                server::Metrics::GetInstance().SearchIndexDataDurationSecondsHistogramObserve(total_time);
                break;
            }
        }
    }

 private:
    int index_type_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class MetricCollector : CollectMetricsBase {
 public:
    MetricCollector() {
        server::Metrics::GetInstance().MetaAccessTotalIncrement();
    }

    ~MetricCollector() {
        auto total_time = TimeFromBegine();
        server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
    }
};

}  // namespace server
}  // namespace milvus
