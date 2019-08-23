/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <cache/GpuCacheMgr.h>
#include "PrometheusMetrics.h"
#include "utils/Log.h"
#include "SystemInfo.h"


namespace zilliz {
namespace milvus {
namespace server {

ServerError
PrometheusMetrics::Init() {
    try {
        ConfigNode &configNode = ServerConfig::GetInstance().GetConfig(CONFIG_METRIC);
        startup_ = configNode.GetValue(CONFIG_METRIC_IS_STARTUP) == "on";
        if(!startup_) return SERVER_SUCCESS;
        // Following should be read from config file.
        const std::string bind_address = configNode.GetChild(CONFIG_PROMETHEUS).GetValue(CONFIG_METRIC_PROMETHEUS_PORT);
        const std::string uri = std::string("/metrics");
        const std::size_t num_threads = 2;

        // Init Exposer
        exposer_ptr_ = std::make_shared<prometheus::Exposer>(bind_address, uri, num_threads);

        // Exposer Registry
        exposer_ptr_->RegisterCollectable(registry_);
    } catch (std::exception& ex) {
        SERVER_LOG_ERROR << "Failed to connect prometheus server: " << std::string(ex.what());
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;

}


void
PrometheusMetrics::CPUUsagePercentSet()  {
    if(!startup_) return ;
    double usage_percent = server::SystemInfo::GetInstance().CPUPercent();
    CPU_usage_percent_.Set(usage_percent);
}

void
PrometheusMetrics::RAMUsagePercentSet() {
    if(!startup_) return ;
    double usage_percent = server::SystemInfo::GetInstance().MemoryPercent();
    RAM_usage_percent_.Set(usage_percent);
}

void
PrometheusMetrics::GPUPercentGaugeSet() {
    if(!startup_) return;
    int numDevice = server::SystemInfo::GetInstance().num_device();
    std::vector<unsigned long long > used_total = server::SystemInfo::GetInstance().GPUMemoryTotal();
    std::vector<unsigned long long > used_memory = server::SystemInfo::GetInstance().GPUMemoryUsed();

    for (int i = 0; i < numDevice; ++i) {
        prometheus::Gauge &GPU_percent = GPU_percent_.Add({{"DeviceNum", std::to_string(i)}});
        double percent = (double)used_memory[i] / (double)used_total[i];
        GPU_percent.Set(percent * 100);
    }
}

void PrometheusMetrics::GPUMemoryUsageGaugeSet() {
    if(!startup_) return;
    std::vector<unsigned long long> values = server::SystemInfo::GetInstance().GPUMemoryUsed();
    constexpr unsigned long long MtoB = 1024*1024;
    int numDevice = server::SystemInfo::GetInstance().num_device();

    for (int i = 0; i < numDevice; ++i) {
        prometheus::Gauge &GPU_memory = GPU_memory_usage_.Add({{"DeviceNum", std::to_string(i)}});
        GPU_memory.Set(values[i] / MtoB);
    }

}
void PrometheusMetrics::AddVectorsPerSecondGaugeSet(int num_vector, int dim, double time) {
    // MB/s
    if(!startup_) return;

    long long MtoB = 1024*1024;
    long long size = num_vector * dim * 4;
    add_vectors_per_second_gauge_.Set(size/time/MtoB);

}
void PrometheusMetrics::QueryIndexTypePerSecondSet(std::string type, double value) {
    if(!startup_) return;
    if(type == "IVF"){
        query_index_IVF_type_per_second_gauge_.Set(value);
    } else if(type == "IDMap"){
        query_index_IDMAP_type_per_second_gauge_.Set(value);
    }

}

void PrometheusMetrics::ConnectionGaugeIncrement() {
    if(!startup_) return;
    connection_gauge_.Increment();
}

void PrometheusMetrics::ConnectionGaugeDecrement() {
    if(!startup_) return;
    connection_gauge_.Decrement();
}

void PrometheusMetrics::OctetsSet() {
    if(!startup_) return;

    // get old stats and reset them
    unsigned long long old_inoctets = SystemInfo::GetInstance().get_inoctets();
    unsigned long long old_outoctets = SystemInfo::GetInstance().get_octets();
    auto old_time = SystemInfo::GetInstance().get_nettime();
    std::pair<unsigned long long, unsigned long long> in_and_out_octets = SystemInfo::GetInstance().Octets();
    SystemInfo::GetInstance().set_inoctets(in_and_out_octets.first);
    SystemInfo::GetInstance().set_outoctets(in_and_out_octets.second);
    SystemInfo::GetInstance().set_nettime();

    //
    constexpr double micro_to_second = 1e-6;
    auto now_time = std::chrono::system_clock::now();
    auto total_microsecond = METRICS_MICROSECONDS(old_time, now_time);
    auto total_second = total_microsecond*micro_to_second;
    if(total_second == 0) return;
    inoctets_gauge_.Set((in_and_out_octets.first-old_inoctets)/total_second);
    outoctets_gauge_.Set((in_and_out_octets.second-old_outoctets)/total_second);
}

void PrometheusMetrics::CPUCoreUsagePercentSet() {
    if (!startup_)
        return;

    std::vector<double> cpu_core_percent = server::SystemInfo::GetInstance().CPUCorePercent();

    for (int i = 0; i < cpu_core_percent.size(); ++i) {
        prometheus::Gauge &core_percent = CPU_.Add({{"CPU", std::to_string(i)}});
        core_percent.Set(cpu_core_percent[i]);
    }
}

void PrometheusMetrics::GPUTemperature() {
    if (!startup_)
        return;

    std::vector<unsigned int> GPU_temperatures = server::SystemInfo::GetInstance().GPUTemperature();

    for (int i = 0; i < GPU_temperatures.size(); ++i) {
        prometheus::Gauge &gpu_temp = GPU_temperature_.Add({{"GPU", std::to_string(i)}});
        gpu_temp.Set(GPU_temperatures[i]);
    }
}

void PrometheusMetrics::CPUTemperature() {
    if (!startup_)
        return;

    std::vector<float> CPU_temperatures = server::SystemInfo::GetInstance().CPUTemperature();

    for (int i = 0; i < CPU_temperatures.size(); ++i) {
        prometheus::Gauge &cpu_temp = CPU_temperature_.Add({{"CPU", std::to_string(i)}});
        cpu_temp.Set(CPU_temperatures[i]);
    }
}

void PrometheusMetrics::GpuCacheUsageGaugeSet(double value) {
    if(!startup_) return;
    int64_t num_processors = server::SystemInfo::GetInstance().num_processor();

    for (auto i = 0; i < num_processors; ++i) {
//        int gpu_cache_usage = cache::GpuCacheMgr::GetInstance(i)->CacheUsage();
//        int gpu_cache_total = cache::GpuCacheMgr::GetInstance(i)->CacheCapacity();
//        prometheus::Gauge &gpu_cache = gpu_cache_usage_.Add({{"GPU_Cache", std::to_string(i)}});
//        gpu_cache.Set(gpu_cache_usage * 100 / gpu_cache_total);
    }
}

}
}
}
