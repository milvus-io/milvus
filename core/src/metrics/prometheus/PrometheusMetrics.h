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

#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/registry.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "metrics/MetricBase.h"
#include "utils/Log.h"
#include "utils/Status.h"

#define METRICS_NOW_TIME std::chrono::system_clock::now()
//#define server::Metrics::GetInstance() server::GetInstance()
#define METRICS_MICROSECONDS(a, b) (std::chrono::duration_cast<std::chrono::microseconds>(b - a)).count();

namespace milvus {
namespace server {

class PrometheusMetrics : public MetricsBase {
 public:
    static PrometheusMetrics&
    GetInstance() {
        static PrometheusMetrics instance;
        return instance;
    }

    Status
    Init() override;

 private:
    std::shared_ptr<prometheus::Gateway> gateway_;
    std::shared_ptr<prometheus::Registry> registry_ = std::make_shared<prometheus::Registry>();
    bool startup_ = false;

 public:
    void
    SetStartup(bool startup) {
        startup_ = startup;
    }

    void
    AddVectorsSuccessTotalIncrement(double value = 1.0) override {
        if (startup_) {
            add_vectors_success_total_.Increment(value);
        }
    }

    void
    AddVectorsFailTotalIncrement(double value = 1.0) override {
        if (startup_) {
            add_vectors_fail_total_.Increment(value);
        }
    }

    void
    AddVectorsDurationHistogramOberve(double value) override {
        if (startup_) {
            add_vectors_duration_histogram_.Observe(value);
        }
    }

    void
    RawFileSizeHistogramObserve(double value) override {
        if (startup_) {
            raw_files_size_histogram_.Observe(value);
        }
    }

    void
    IndexFileSizeHistogramObserve(double value) override {
        if (startup_) {
            index_files_size_histogram_.Observe(value);
        }
    }

    void
    BuildIndexDurationSecondsHistogramObserve(double value) override {
        if (startup_) {
            build_index_duration_seconds_histogram_.Observe(value);
        }
    }

    void
    CpuCacheUsageGaugeSet(double value) override {
        if (startup_) {
            cpu_cache_usage_gauge_.Set(value);
        }
    }

    void
    GpuCacheUsageGaugeSet() override;

    void
    MetaAccessTotalIncrement(double value = 1) override {
        if (startup_) {
            meta_access_total_.Increment(value);
        }
    }

    void
    MetaAccessDurationSecondsHistogramObserve(double value) override {
        if (startup_) {
            meta_access_duration_seconds_histogram_.Observe(value);
        }
    }

    void
    FaissDiskLoadDurationSecondsHistogramObserve(double value) override {
        if (startup_) {
            faiss_disk_load_duration_seconds_histogram_.Observe(value);
        }
    }

    void
    FaissDiskLoadSizeBytesHistogramObserve(double value) override {
        if (startup_) {
            faiss_disk_load_size_bytes_histogram_.Observe(value);
        }
    }

    void
    FaissDiskLoadIOSpeedGaugeSet(double value) override {
        if (startup_) {
            faiss_disk_load_IO_speed_gauge_.Set(value);
        }
    }

    void
    CacheAccessTotalIncrement(double value = 1) override {
        if (startup_) {
            cache_access_total_.Increment(value);
        }
    }

    void
    MemTableMergeDurationSecondsHistogramObserve(double value) override {
        if (startup_) {
            mem_table_merge_duration_seconds_histogram_.Observe(value);
        }
    }

    void
    SearchIndexDataDurationSecondsHistogramObserve(double value) override {
        if (startup_) {
            search_index_data_duration_seconds_histogram_.Observe(value);
        }
    }

    void
    SearchRawDataDurationSecondsHistogramObserve(double value) override {
        if (startup_) {
            search_raw_data_duration_seconds_histogram_.Observe(value);
        }
    }

    void
    IndexFileSizeTotalIncrement(double value = 1) override {
        if (startup_) {
            index_file_size_total_.Increment(value);
        }
    }

    void
    RawFileSizeTotalIncrement(double value = 1) override {
        if (startup_) {
            raw_file_size_total_.Increment(value);
        }
    }

    void
    IndexFileSizeGaugeSet(double value) override {
        if (startup_) {
            index_file_size_gauge_.Set(value);
        }
    }

    void
    RawFileSizeGaugeSet(double value) override {
        if (startup_) {
            raw_file_size_gauge_.Set(value);
        }
    }

    void
    QueryResponseSummaryObserve(double value) override {
        if (startup_) {
            query_response_summary_.Observe(value);
        }
    }

    void
    DiskStoreIOSpeedGaugeSet(double value) override {
        if (startup_) {
            disk_store_IO_speed_gauge_.Set(value);
        }
    }

    void
    DataFileSizeGaugeSet(double value) override {
        if (startup_) {
            data_file_size_gauge_.Set(value);
        }
    }

    void
    AddVectorsSuccessGaugeSet(double value) override {
        if (startup_) {
            add_vectors_success_gauge_.Set(value);
        }
    }

    void
    AddVectorsFailGaugeSet(double value) override {
        if (startup_) {
            add_vectors_fail_gauge_.Set(value);
        }
    }

    void
    QueryVectorResponseSummaryObserve(double value, int count = 1) override {
        if (startup_) {
            for (int i = 0; i < count; ++i) {
                query_vector_response_summary_.Observe(value);
            }
        }
    }

    void
    QueryVectorResponsePerSecondGaugeSet(double value) override {
        if (startup_) {
            query_vector_response_per_second_gauge_.Set(value);
        }
    }

    void
    CPUUsagePercentSet() override;
    void
    CPUCoreUsagePercentSet() override;

    void
    RAMUsagePercentSet() override;

    void
    QueryResponsePerSecondGaugeSet(double value) override {
        if (startup_) {
            query_response_per_second_gauge.Set(value);
        }
    }

    void
    GPUPercentGaugeSet() override;
    void
    GPUMemoryUsageGaugeSet() override;
    void
    AddVectorsPerSecondGaugeSet(int num_vector, int dim, double time) override;
    void
    QueryIndexTypePerSecondSet(std::string type, double value) override;
    void
    ConnectionGaugeIncrement() override;
    void
    ConnectionGaugeDecrement() override;

    void
    KeepingAliveCounterIncrement(double value = 1) override {
        if (startup_) {
            keeping_alive_counter_.Increment(value);
        }
    }

    void
    OctetsSet() override;

    void
    GPUTemperature() override;
    void
    CPUTemperature() override;

    void
    PushToGateway() override {
        if (startup_) {
            if (gateway_->Push() != 200) {
                LOG_ENGINE_WARNING_ << "Metrics pushgateway failed";
            }
        }
    }

    std::shared_ptr<prometheus::Gateway>&
    gateway() {
        return gateway_;
    }

    //    prometheus::Exposer& exposer() { return exposer_;}
    std::shared_ptr<prometheus::Registry>&
    registry_ptr() {
        return registry_;
    }

    // .....
 private:
    ////all from db_connection.cpp
    //    prometheus::Family<prometheus::Counter> &connect_request_ = prometheus::BuildCounter()
    //        .Name("connection_total")
    //        .Help("total number of connection has been made")
    //        .Register(*registry_);
    //    prometheus::Counter &connection_total_ = connect_request_.Add({});

    ////all from DBImpl.cpp
    using BucketBoundaries = std::vector<double>;
    // record add_group request
    prometheus::Family<prometheus::Counter>& add_group_request_ = prometheus::BuildCounter()
                                                                      .Name("add_group_request_total")
                                                                      .Help("the number of add_group request")
                                                                      .Register(*registry_);

    prometheus::Counter& add_group_success_total_ = add_group_request_.Add({{"outcome", "success"}});
    prometheus::Counter& add_group_fail_total_ = add_group_request_.Add({{"outcome", "fail"}});

    // record get_group request
    prometheus::Family<prometheus::Counter>& get_group_request_ = prometheus::BuildCounter()
                                                                      .Name("get_group_request_total")
                                                                      .Help("the number of get_group request")
                                                                      .Register(*registry_);

    prometheus::Counter& get_group_success_total_ = get_group_request_.Add({{"outcome", "success"}});
    prometheus::Counter& get_group_fail_total_ = get_group_request_.Add({{"outcome", "fail"}});

    // record has_group request
    prometheus::Family<prometheus::Counter>& has_group_request_ = prometheus::BuildCounter()
                                                                      .Name("has_group_request_total")
                                                                      .Help("the number of has_group request")
                                                                      .Register(*registry_);

    prometheus::Counter& has_group_success_total_ = has_group_request_.Add({{"outcome", "success"}});
    prometheus::Counter& has_group_fail_total_ = has_group_request_.Add({{"outcome", "fail"}});

    // record get_group_files
    prometheus::Family<prometheus::Counter>& get_group_files_request_ =
        prometheus::BuildCounter()
            .Name("get_group_files_request_total")
            .Help("the number of get_group_files request")
            .Register(*registry_);

    prometheus::Counter& get_group_files_success_total_ = get_group_files_request_.Add({{"outcome", "success"}});
    prometheus::Counter& get_group_files_fail_total_ = get_group_files_request_.Add({{"outcome", "fail"}});

    // record add_vectors count and average time
    // need to be considered
    prometheus::Family<prometheus::Counter>& add_vectors_request_ = prometheus::BuildCounter()
                                                                        .Name("add_vectors_request_total")
                                                                        .Help("the number of vectors added")
                                                                        .Register(*registry_);
    prometheus::Counter& add_vectors_success_total_ = add_vectors_request_.Add({{"outcome", "success"}});
    prometheus::Counter& add_vectors_fail_total_ = add_vectors_request_.Add({{"outcome", "fail"}});

    prometheus::Family<prometheus::Histogram>& add_vectors_duration_seconds_ =
        prometheus::BuildHistogram()
            .Name("add_vector_duration_microseconds")
            .Help("average time of adding every vector")
            .Register(*registry_);
    prometheus::Histogram& add_vectors_duration_histogram_ =
        add_vectors_duration_seconds_.Add({}, BucketBoundaries{0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.08, 0.1, 0.5, 1});

    // record search count and average time
    prometheus::Family<prometheus::Counter>& search_request_ = prometheus::BuildCounter()
                                                                   .Name("search_request_total")
                                                                   .Help("the number of search request")
                                                                   .Register(*registry_);
    prometheus::Counter& search_success_total_ = search_request_.Add({{"outcome", "success"}});
    prometheus::Counter& search_fail_total_ = search_request_.Add({{"outcome", "fail"}});

    prometheus::Family<prometheus::Histogram>& search_request_duration_seconds_ =
        prometheus::BuildHistogram()
            .Name("search_request_duration_microsecond")
            .Help("histogram of processing time for each search")
            .Register(*registry_);
    prometheus::Histogram& search_duration_histogram_ =
        search_request_duration_seconds_.Add({}, BucketBoundaries{0.1, 1.0, 10.0});

    // record raw_files size histogram
    prometheus::Family<prometheus::Histogram>& raw_files_size_ = prometheus::BuildHistogram()
                                                                     .Name("search_raw_files_bytes")
                                                                     .Help("histogram of raw files size by bytes")
                                                                     .Register(*registry_);
    prometheus::Histogram& raw_files_size_histogram_ =
        raw_files_size_.Add({}, BucketBoundaries{1e9, 2e9, 4e9, 6e9, 8e9, 1e10});

    // record index_files size histogram
    prometheus::Family<prometheus::Histogram>& index_files_size_ = prometheus::BuildHistogram()
                                                                       .Name("search_index_files_bytes")
                                                                       .Help("histogram of index files size by bytes")
                                                                       .Register(*registry_);
    prometheus::Histogram& index_files_size_histogram_ =
        index_files_size_.Add({}, BucketBoundaries{1e9, 2e9, 4e9, 6e9, 8e9, 1e10});

    // record index and raw files size counter
    prometheus::Family<prometheus::Counter>& file_size_total_ = prometheus::BuildCounter()
                                                                    .Name("search_file_size_total")
                                                                    .Help("searched index and raw file size")
                                                                    .Register(*registry_);
    prometheus::Counter& index_file_size_total_ = file_size_total_.Add({{"type", "index"}});
    prometheus::Counter& raw_file_size_total_ = file_size_total_.Add({{"type", "raw"}});

    // record index and raw files size counter
    prometheus::Family<prometheus::Gauge>& file_size_gauge_ = prometheus::BuildGauge()
                                                                  .Name("search_file_size_gauge")
                                                                  .Help("searched current index and raw file size")
                                                                  .Register(*registry_);
    prometheus::Gauge& index_file_size_gauge_ = file_size_gauge_.Add({{"type", "index"}});
    prometheus::Gauge& raw_file_size_gauge_ = file_size_gauge_.Add({{"type", "raw"}});

    // record processing time for building index
    prometheus::Family<prometheus::Histogram>& build_index_duration_seconds_ =
        prometheus::BuildHistogram()
            .Name("build_index_duration_microseconds")
            .Help("histogram of processing time for building index")
            .Register(*registry_);
    prometheus::Histogram& build_index_duration_seconds_histogram_ =
        build_index_duration_seconds_.Add({}, BucketBoundaries{5e5, 2e6, 4e6, 6e6, 8e6, 1e7});

    // record processing time for all building index
    prometheus::Family<prometheus::Histogram>& all_build_index_duration_seconds_ =
        prometheus::BuildHistogram()
            .Name("all_build_index_duration_microseconds")
            .Help("histogram of processing time for building index")
            .Register(*registry_);
    prometheus::Histogram& all_build_index_duration_seconds_histogram_ =
        all_build_index_duration_seconds_.Add({}, BucketBoundaries{2e6, 4e6, 6e6, 8e6, 1e7});

    // record duration of merging mem collection
    prometheus::Family<prometheus::Histogram>& mem_table_merge_duration_seconds_ =
        prometheus::BuildHistogram()
            .Name("mem_table_merge_duration_microseconds")
            .Help("histogram of processing time for merging mem tables")
            .Register(*registry_);
    prometheus::Histogram& mem_table_merge_duration_seconds_histogram_ =
        mem_table_merge_duration_seconds_.Add({}, BucketBoundaries{5e4, 1e5, 2e5, 4e5, 6e5, 8e5, 1e6});

    // record search index and raw data duration
    prometheus::Family<prometheus::Histogram>& search_data_duration_seconds_ =
        prometheus::BuildHistogram()
            .Name("search_data_duration_microseconds")
            .Help("histograms of processing time for search index and raw data")
            .Register(*registry_);
    prometheus::Histogram& search_index_data_duration_seconds_histogram_ =
        search_data_duration_seconds_.Add({{"type", "index"}}, BucketBoundaries{1e5, 2e5, 4e5, 6e5, 8e5});
    prometheus::Histogram& search_raw_data_duration_seconds_histogram_ =
        search_data_duration_seconds_.Add({{"type", "raw"}}, BucketBoundaries{1e5, 2e5, 4e5, 6e5, 8e5});

    ////all form Cache.cpp
    // record cache usage, when insert/erase/clear/free

    ////all from Meta.cpp
    // record meta visit count and time
    //    prometheus::Family<prometheus::Counter> &meta_visit_ = prometheus::BuildCounter()
    //        .Name("meta_visit_total")
    //        .Help("the number of accessing Meta")
    //        .Register(*registry_);
    //    prometheus::Counter &meta_visit_total_ = meta_visit_.Add({{}});
    //
    //    prometheus::Family<prometheus::Histogram> &meta_visit_duration_seconds_ = prometheus::BuildHistogram()
    //        .Name("meta_visit_duration_seconds")
    //        .Help("histogram of processing time to get data from mata")
    //        .Register(*registry_);
    //    prometheus::Histogram &meta_visit_duration_seconds_histogram_ =
    //          meta_visit_duration_seconds_.Add({{}}, BucketBoundaries{0.1, 1.0, 10.0});

    ////all from MemManager.cpp
    // record memory usage percent
    prometheus::Family<prometheus::Gauge>& mem_usage_percent_ =
        prometheus::BuildGauge().Name("memory_usage_percent").Help("memory usage percent").Register(*registry_);
    prometheus::Gauge& mem_usage_percent_gauge_ = mem_usage_percent_.Add({});

    // record memory usage toal
    prometheus::Family<prometheus::Gauge>& mem_usage_total_ =
        prometheus::BuildGauge().Name("memory_usage_total").Help("memory usage total").Register(*registry_);
    prometheus::Gauge& mem_usage_total_gauge_ = mem_usage_total_.Add({});

    ////all from DBMetaImpl.cpp
    // record meta access count
    prometheus::Family<prometheus::Counter>& meta_access_ =
        prometheus::BuildCounter().Name("meta_access_total").Help("the number of meta accessing").Register(*registry_);
    prometheus::Counter& meta_access_total_ = meta_access_.Add({});

    // record meta access duration
    prometheus::Family<prometheus::Histogram>& meta_access_duration_seconds_ =
        prometheus::BuildHistogram()
            .Name("meta_access_duration_microseconds")
            .Help("histogram of processing time for accessing mata")
            .Register(*registry_);
    prometheus::Histogram& meta_access_duration_seconds_histogram_ =
        meta_access_duration_seconds_.Add({}, BucketBoundaries{100, 300, 500, 700, 900, 2000, 4000, 6000, 8000, 20000});

    ////all from FaissExecutionEngine.cpp
    // record data loading from disk count, size, duration, IO speed
    prometheus::Family<prometheus::Histogram>& disk_load_duration_second_ =
        prometheus::BuildHistogram()
            .Name("disk_load_duration_microseconds")
            .Help("Histogram of processing time for loading data from disk")
            .Register(*registry_);
    prometheus::Histogram& faiss_disk_load_duration_seconds_histogram_ =
        disk_load_duration_second_.Add({{"DB", "Faiss"}}, BucketBoundaries{2e5, 4e5, 6e5, 8e5});

    prometheus::Family<prometheus::Histogram>& disk_load_size_bytes_ =
        prometheus::BuildHistogram()
            .Name("disk_load_size_bytes")
            .Help("Histogram of data size by bytes for loading data from disk")
            .Register(*registry_);
    prometheus::Histogram& faiss_disk_load_size_bytes_histogram_ =
        disk_load_size_bytes_.Add({{"DB", "Faiss"}}, BucketBoundaries{1e9, 2e9, 4e9, 6e9, 8e9});

    //    prometheus::Family<prometheus::Histogram> &disk_load_IO_speed_ = prometheus::BuildHistogram()
    //        .Name("disk_load_IO_speed_byte_per_sec")
    //        .Help("Histogram of IO speed for loading data from disk")
    //        .Register(*registry_);
    //    prometheus::Histogram &faiss_disk_load_IO_speed_histogram_ =
    //          disk_load_IO_speed_.Add({{"DB","Faiss"}},BucketBoundaries{1000, 2000, 3000, 4000, 6000, 8000});

    prometheus::Family<prometheus::Gauge>& faiss_disk_load_IO_speed_ = prometheus::BuildGauge()
                                                                           .Name("disk_load_IO_speed_byte_per_microsec")
                                                                           .Help("disk IO speed ")
                                                                           .Register(*registry_);
    prometheus::Gauge& faiss_disk_load_IO_speed_gauge_ = faiss_disk_load_IO_speed_.Add({{"DB", "Faiss"}});

    ////all from CacheMgr.cpp
    // record cache access count
    prometheus::Family<prometheus::Counter>& cache_access_ = prometheus::BuildCounter()
                                                                 .Name("cache_access_total")
                                                                 .Help("the count of accessing cache ")
                                                                 .Register(*registry_);
    prometheus::Counter& cache_access_total_ = cache_access_.Add({});

    // record CPU cache usage and %
    prometheus::Family<prometheus::Gauge>& cpu_cache_usage_ =
        prometheus::BuildGauge().Name("cache_usage_bytes").Help("current cache usage by bytes").Register(*registry_);
    prometheus::Gauge& cpu_cache_usage_gauge_ = cpu_cache_usage_.Add({});

    // record GPU cache usage and %
    prometheus::Family<prometheus::Gauge>& gpu_cache_usage_ = prometheus::BuildGauge()
                                                                  .Name("gpu_cache_usage_bytes")
                                                                  .Help("current gpu cache usage by bytes")
                                                                  .Register(*registry_);

    // record query response
    using Quantiles = std::vector<prometheus::detail::CKMSQuantiles::Quantile>;
    prometheus::Family<prometheus::Summary>& query_response_ =
        prometheus::BuildSummary().Name("query_response_summary").Help("query response summary").Register(*registry_);
    prometheus::Summary& query_response_summary_ =
        query_response_.Add({}, Quantiles{{0.95, 0.00}, {0.9, 0.05}, {0.8, 0.1}});

    prometheus::Family<prometheus::Summary>& query_vector_response_ = prometheus::BuildSummary()
                                                                          .Name("query_vector_response_summary")
                                                                          .Help("query each vector response summary")
                                                                          .Register(*registry_);
    prometheus::Summary& query_vector_response_summary_ =
        query_vector_response_.Add({}, Quantiles{{0.95, 0.00}, {0.9, 0.05}, {0.8, 0.1}});

    prometheus::Family<prometheus::Gauge>& query_vector_response_per_second_ =
        prometheus::BuildGauge()
            .Name("query_vector_response_per_microsecond")
            .Help("the number of vectors can be queried every second ")
            .Register(*registry_);
    prometheus::Gauge& query_vector_response_per_second_gauge_ = query_vector_response_per_second_.Add({});

    prometheus::Family<prometheus::Gauge>& query_response_per_second_ =
        prometheus::BuildGauge()
            .Name("query_response_per_microsecond")
            .Help("the number of queries can be processed every microsecond")
            .Register(*registry_);
    prometheus::Gauge& query_response_per_second_gauge = query_response_per_second_.Add({});

    prometheus::Family<prometheus::Gauge>& disk_store_IO_speed_ =
        prometheus::BuildGauge()
            .Name("disk_store_IO_speed_bytes_per_microseconds")
            .Help("disk_store_IO_speed")
            .Register(*registry_);
    prometheus::Gauge& disk_store_IO_speed_gauge_ = disk_store_IO_speed_.Add({});

    prometheus::Family<prometheus::Gauge>& data_file_size_ =
        prometheus::BuildGauge().Name("data_file_size_bytes").Help("data file size by bytes").Register(*registry_);
    prometheus::Gauge& data_file_size_gauge_ = data_file_size_.Add({});

    prometheus::Family<prometheus::Gauge>& add_vectors_ =
        prometheus::BuildGauge().Name("add_vectors").Help("current added vectors").Register(*registry_);
    prometheus::Gauge& add_vectors_success_gauge_ = add_vectors_.Add({{"outcome", "success"}});
    prometheus::Gauge& add_vectors_fail_gauge_ = add_vectors_.Add({{"outcome", "fail"}});

    prometheus::Family<prometheus::Gauge>& add_vectors_per_second_ = prometheus::BuildGauge()
                                                                         .Name("add_vectors_throughput_per_microsecond")
                                                                         .Help("add vectors throughput per microsecond")
                                                                         .Register(*registry_);
    prometheus::Gauge& add_vectors_per_second_gauge_ = add_vectors_per_second_.Add({});

    prometheus::Family<prometheus::Gauge>& CPU_ = prometheus::BuildGauge()
                                                      .Name("CPU_usage_percent")
                                                      .Help("CPU usage percent by this this process")
                                                      .Register(*registry_);
    prometheus::Gauge& CPU_usage_percent_ = CPU_.Add({{"CPU", "avg"}});

    prometheus::Family<prometheus::Gauge>& RAM_ = prometheus::BuildGauge()
                                                      .Name("RAM_usage_percent")
                                                      .Help("RAM usage percent by this process")
                                                      .Register(*registry_);
    prometheus::Gauge& RAM_usage_percent_ = RAM_.Add({});

    // GPU Usage Percent
    prometheus::Family<prometheus::Gauge>& GPU_percent_ =
        prometheus::BuildGauge().Name("Gpu_usage_percent").Help("GPU_usage_percent ").Register(*registry_);

    // GPU Mempry used
    prometheus::Family<prometheus::Gauge>& GPU_memory_usage_ =
        prometheus::BuildGauge().Name("GPU_memory_usage_total").Help("GPU memory usage total ").Register(*registry_);

    prometheus::Family<prometheus::Gauge>& query_index_type_per_second_ =
        prometheus::BuildGauge()
            .Name("query_index_throughtout_per_microsecond")
            .Help("query index throughtout per microsecond")
            .Register(*registry_);
    prometheus::Gauge& query_index_IVF_type_per_second_gauge_ =
        query_index_type_per_second_.Add({{"IndexType", "IVF"}});
    prometheus::Gauge& query_index_IDMAP_type_per_second_gauge_ =
        query_index_type_per_second_.Add({{"IndexType", "IDMAP"}});

    prometheus::Family<prometheus::Gauge>& connection_ =
        prometheus::BuildGauge().Name("connection_number").Help("the number of connections").Register(*registry_);
    prometheus::Gauge& connection_gauge_ = connection_.Add({});

    prometheus::Family<prometheus::Counter>& keeping_alive_ = prometheus::BuildCounter()
                                                                  .Name("keeping_alive_seconds_total")
                                                                  .Help("total seconds of the serve alive")
                                                                  .Register(*registry_);
    prometheus::Counter& keeping_alive_counter_ = keeping_alive_.Add({});

    prometheus::Family<prometheus::Gauge>& octets_ =
        prometheus::BuildGauge().Name("octets_bytes_per_second").Help("octets bytes per second").Register(*registry_);
    prometheus::Gauge& inoctets_gauge_ = octets_.Add({{"type", "inoctets"}});
    prometheus::Gauge& outoctets_gauge_ = octets_.Add({{"type", "outoctets"}});

    prometheus::Family<prometheus::Gauge>& GPU_temperature_ =
        prometheus::BuildGauge().Name("GPU_temperature").Help("GPU temperature").Register(*registry_);

    prometheus::Family<prometheus::Gauge>& CPU_temperature_ =
        prometheus::BuildGauge().Name("CPU_temperature").Help("CPU temperature").Register(*registry_);
};

}  // namespace server
}  // namespace milvus
