/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MetricBase.h"
#include "db/meta/MetaTypes.h"


namespace zilliz {
namespace milvus {
namespace server {

#define METRICS_NOW_TIME std::chrono::system_clock::now()
#define METRICS_MICROSECONDS(a, b) (std::chrono::duration_cast<std::chrono::microseconds> (b-a)).count();

enum class MetricCollectorType {
    INVALID,
    PROMETHEUS,
    ZABBIX
};

class Metrics {
 public:
    static MetricsBase &GetInstance();

 private:
    static MetricsBase &CreateMetricsCollector();
};

class CollectInsertMetrics {
public:
    CollectInsertMetrics(size_t n, engine::Status& status) : n_(n), status_(status) {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectInsertMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        double avg_time = total_time / n_;
        for (int i = 0; i < n_; ++i) {
            Metrics::GetInstance().AddVectorsDurationHistogramOberve(avg_time);
        }

        //    server::Metrics::GetInstance().add_vector_duration_seconds_quantiles().Observe((average_time));
        if (status_.ok()) {
            server::Metrics::GetInstance().AddVectorsSuccessTotalIncrement(n_);
            server::Metrics::GetInstance().AddVectorsSuccessGaugeSet(n_);
        }
        else {
            server::Metrics::GetInstance().AddVectorsFailTotalIncrement(n_);
            server::Metrics::GetInstance().AddVectorsFailGaugeSet(n_);
        }
    }

private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
    size_t n_;
    engine::Status& status_;
};

class CollectQueryMetrics {
public:
    CollectQueryMetrics(size_t nq) : nq_(nq) {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectQueryMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        for (int i = 0; i < nq_; ++i) {
            server::Metrics::GetInstance().QueryResponseSummaryObserve(total_time);
        }
        auto average_time = total_time / nq_;
        server::Metrics::GetInstance().QueryVectorResponseSummaryObserve(average_time, nq_);
        server::Metrics::GetInstance().QueryVectorResponsePerSecondGaugeSet(double (nq_) / total_time);
    }

private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
    size_t nq_;
};

class CollectMergeFilesMetrics {
public:
    CollectMergeFilesMetrics() {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectMergeFilesMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        server::Metrics::GetInstance().MemTableMergeDurationSecondsHistogramObserve(total_time);
    }

private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
};

class CollectBuildIndexMetrics {
public:
    CollectBuildIndexMetrics() {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectBuildIndexMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        server::Metrics::GetInstance().BuildIndexDurationSecondsHistogramObserve(total_time);
    }
private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
};

class CollectExecutionEngineMetrics {
public:
    CollectExecutionEngineMetrics(double physical_size) : physical_size_(physical_size) {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectExecutionEngineMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);

        server::Metrics::GetInstance().FaissDiskLoadDurationSecondsHistogramObserve(total_time);

        server::Metrics::GetInstance().FaissDiskLoadSizeBytesHistogramObserve(physical_size_);
        server::Metrics::GetInstance().FaissDiskLoadIOSpeedGaugeSet(physical_size_ / double(total_time));
    }

private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
    double physical_size_;
};

class CollectSerializeMetrics {
public:
    CollectSerializeMetrics(size_t size) : size_(size) {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectSerializeMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        server::Metrics::GetInstance().DiskStoreIOSpeedGaugeSet((double) size_ / total_time);
    }
private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
    size_t size_;
};

class CollectorAddMetrics {
public:
    CollectorAddMetrics(size_t n, uint16_t dimension) : n_(n), dimension_(dimension) {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectorAddMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        server::Metrics::GetInstance().AddVectorsPerSecondGaugeSet(static_cast<int>(n_),
                                                                   static_cast<int>(dimension_),
                                                                   total_time);
    }
private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
    size_t n_;
    uint16_t dimension_;
};

class CollectorDurationMetrics {
public:
    CollectorDurationMetrics() {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectorDurationMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        switch (index_type_) {
            case engine::meta::TableFileSchema::RAW: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            case engine::meta::TableFileSchema::TO_INDEX: {
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
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
    int index_type_;
};

class CollectSearchTaskMetrics {
public:
    CollectSearchTaskMetrics(int index_type) : index_type_(index_type) {
        start_time_ = METRICS_NOW_TIME;
    }

    ~CollectSearchTaskMetrics() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        switch(index_type_) {
            case engine::meta::TableFileSchema::RAW: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            case engine::meta::TableFileSchema::TO_INDEX: {
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
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
    int index_type_;
};

class MetricCollector {
public:
    MetricCollector() {
        server::Metrics::GetInstance().MetaAccessTotalIncrement();
        start_time_ = METRICS_NOW_TIME;
    }

    ~MetricCollector() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
    }

private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
};



}
}
}



