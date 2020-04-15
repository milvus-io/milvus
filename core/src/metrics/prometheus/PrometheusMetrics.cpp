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

#include "metrics/prometheus/PrometheusMetrics.h"
#include "cache/GpuCacheMgr.h"
#include "config/Config.h"
#include "metrics/SystemInfo.h"
#include "utils/Log.h"

#include <string>
#include <utility>

namespace milvus {
namespace server {

Status
PrometheusMetrics::Init() {
    try {
        Config& config = Config::GetInstance();
        CONFIG_CHECK(config.GetMetricConfigEnableMonitor(startup_));
        if (!startup_) {
            return Status::OK();
        }

        // Following should be read from config file.
        std::string push_port, push_address;
        CONFIG_CHECK(config.GetMetricConfigPort(push_port));
        CONFIG_CHECK(config.GetMetricConfigAddress(push_address));

        const std::string uri = std::string("/metrics");
        // const std::size_t num_threads = 2;

        auto labels = prometheus::Gateway::GetInstanceLabel("pushgateway");

        // Init pushgateway
        gateway_ = std::make_shared<prometheus::Gateway>(push_address, push_port, "milvus_metrics", labels);

        // Init Exposer
        // exposer_ptr_ = std::make_shared<prometheus::Exposer>(bind_address, uri, num_threads);

        // Pushgateway Registry
        gateway_->RegisterCollectable(registry_);
    } catch (std::exception& ex) {
        LOG_SERVER_ERROR_ << "Failed to connect prometheus server: " << std::string(ex.what());
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

void
PrometheusMetrics::CPUUsagePercentSet() {
    if (!startup_) {
        return;
    }

    double usage_percent = server::SystemInfo::GetInstance().CPUPercent();
    CPU_usage_percent_.Set(usage_percent);
}

void
PrometheusMetrics::RAMUsagePercentSet() {
    if (!startup_) {
        return;
    }

    double usage_percent = server::SystemInfo::GetInstance().MemoryPercent();
    RAM_usage_percent_.Set(usage_percent);
}

void
PrometheusMetrics::GPUPercentGaugeSet() {
    if (!startup_) {
        return;
    }

    int numDevice = server::SystemInfo::GetInstance().num_device();
    std::vector<uint64_t> used_total = server::SystemInfo::GetInstance().GPUMemoryTotal();
    std::vector<uint64_t> used_memory = server::SystemInfo::GetInstance().GPUMemoryUsed();

    for (int i = 0; i < numDevice; ++i) {
        prometheus::Gauge& GPU_percent = GPU_percent_.Add({{"DeviceNum", std::to_string(i)}});
        double percent = (double)used_memory[i] / (double)used_total[i];
        GPU_percent.Set(percent * 100);
    }
}

void
PrometheusMetrics::GPUMemoryUsageGaugeSet() {
    if (!startup_) {
        return;
    }

    std::vector<uint64_t> values = server::SystemInfo::GetInstance().GPUMemoryUsed();
    constexpr uint64_t MtoB = 1024 * 1024;
    int numDevice = server::SystemInfo::GetInstance().num_device();

    for (int i = 0; i < numDevice; ++i) {
        prometheus::Gauge& GPU_memory = GPU_memory_usage_.Add({{"DeviceNum", std::to_string(i)}});
        GPU_memory.Set(values[i] / MtoB);
    }
}

void
PrometheusMetrics::AddVectorsPerSecondGaugeSet(int num_vector, int dim, double time) {
    // MB/s
    if (!startup_) {
        return;
    }

    int64_t MtoB = 1024 * 1024;
    int64_t size = num_vector * dim * 4;
    add_vectors_per_second_gauge_.Set(size / time / MtoB);
}

void
PrometheusMetrics::QueryIndexTypePerSecondSet(std::string type, double value) {
    if (!startup_) {
        return;
    }

    if (type == "IVF") {
        query_index_IVF_type_per_second_gauge_.Set(value);
    } else if (type == "IDMap") {
        query_index_IDMAP_type_per_second_gauge_.Set(value);
    }
}

void
PrometheusMetrics::ConnectionGaugeIncrement() {
    if (!startup_) {
        return;
    }

    connection_gauge_.Increment();
}

void
PrometheusMetrics::ConnectionGaugeDecrement() {
    if (!startup_) {
        return;
    }

    connection_gauge_.Decrement();
}

void
PrometheusMetrics::OctetsSet() {
    if (!startup_) {
        return;
    }

    // get old stats and reset them
    uint64_t old_inoctets = SystemInfo::GetInstance().get_inoctets();
    uint64_t old_outoctets = SystemInfo::GetInstance().get_octets();
    auto old_time = SystemInfo::GetInstance().get_nettime();
    std::pair<uint64_t, uint64_t> in_and_out_octets = SystemInfo::GetInstance().Octets();
    SystemInfo::GetInstance().set_inoctets(in_and_out_octets.first);
    SystemInfo::GetInstance().set_outoctets(in_and_out_octets.second);
    SystemInfo::GetInstance().set_nettime();

    //
    constexpr double micro_to_second = 1e-6;
    auto now_time = std::chrono::system_clock::now();
    auto total_microsecond = METRICS_MICROSECONDS(old_time, now_time);
    auto total_second = total_microsecond * micro_to_second;
    if (total_second == 0) {
        return;
    }

    inoctets_gauge_.Set((in_and_out_octets.first - old_inoctets) / total_second);
    outoctets_gauge_.Set((in_and_out_octets.second - old_outoctets) / total_second);
}

void
PrometheusMetrics::CPUCoreUsagePercentSet() {
    if (!startup_) {
        return;
    }

    std::vector<double> cpu_core_percent = server::SystemInfo::GetInstance().CPUCorePercent();

    for (int i = 0; i < cpu_core_percent.size(); ++i) {
        prometheus::Gauge& core_percent = CPU_.Add({{"CPU", std::to_string(i)}});
        core_percent.Set(cpu_core_percent[i]);
    }
}

void
PrometheusMetrics::GPUTemperature() {
    if (!startup_) {
        return;
    }

    std::vector<uint64_t> GPU_temperatures = server::SystemInfo::GetInstance().GPUTemperature();

    for (int i = 0; i < GPU_temperatures.size(); ++i) {
        prometheus::Gauge& gpu_temp = GPU_temperature_.Add({{"GPU", std::to_string(i)}});
        gpu_temp.Set(GPU_temperatures[i]);
    }
}

void
PrometheusMetrics::CPUTemperature() {
    if (!startup_) {
        return;
    }

    std::vector<float> CPU_temperatures = server::SystemInfo::GetInstance().CPUTemperature();

    float avg_cpu_temp = 0;
    for (int i = 0; i < CPU_temperatures.size(); ++i) {
        avg_cpu_temp += CPU_temperatures[i];
    }
    avg_cpu_temp /= CPU_temperatures.size();

    prometheus::Gauge& cpu_temp = CPU_temperature_.Add({{"CPU", std::to_string(0)}});
    cpu_temp.Set(avg_cpu_temp);

    //    for (int i = 0; i < CPU_temperatures.size(); ++i) {
    //        prometheus::Gauge& cpu_temp = CPU_temperature_.Add({{"CPU", std::to_string(i)}});
    //        cpu_temp.Set(CPU_temperatures[i]);
    //    }
}

void
PrometheusMetrics::GpuCacheUsageGaugeSet() {
    //    std::vector<uint64_t > gpu_ids = {0};
    //    for(auto i = 0; i < gpu_ids.size(); ++i) {
    //        uint64_t cache_usage = cache::GpuCacheMgr::GetInstance(gpu_ids[i])->CacheUsage();
    //        uint64_t cache_capacity = cache::GpuCacheMgr::GetInstance(gpu_ids[i])->CacheCapacity();
    //        prometheus::Gauge &gpu_cache = gpu_cache_usage_.Add({{"GPU_Cache", std::to_string(i)}});
    //        gpu_cache.Set(cache_usage * 100 / cache_capacity);
    //    }
}

}  // namespace server
}  // namespace milvus
