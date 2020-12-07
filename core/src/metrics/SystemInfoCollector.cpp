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

#include <unistd.h>
#include <cmath>

#include "db/Constants.h"
#include "metrics/SystemInfo.h"
#include "metrics/SystemInfoCollector.h"

namespace milvus {

void
SystemInfoCollector::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
        return;
    }
    running_ = true;
    collector_thread_ = std::thread(&SystemInfoCollector::collector_function, this);
}

void
SystemInfoCollector::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (not running_) {
        return;
    }
    running_ = false;
    collector_thread_.join();
}

void
SystemInfoCollector::collector_function() {
    base_network_in_octets_ = SystemInfo::NetworkInOctets();
    base_network_out_octets_ = SystemInfo::NetworkOutOctets();
    SystemInfo::CpuUtilizationRatio(base_cpu_, base_sys_cpu_, base_user_cpu_);
    while (running_) {
        /* collect metrics */
        keeping_alive_counter_.Increment(1);

        // cpu_utilization_ratio range: 0~25600%
        cpu_utilization_ratio_.Set(cpu_utilization_ratio());

        // cpu_temperature range: -40°C~120°C
        cpu_temperature_.Set(cpu_temperature_celsius());

        // mem_usage range: 0~1TB
        mem_usage_.Set(mem_usage());

        // mem_available range: 0~1TB
        mem_available_.Set(mem_available());

        // network_receive_bytes_total range: 0~1GB
        network_receive_bytes_total_.Set(network_receive_total());

        // network_transport_bytes_total range: 0~1GB
        network_transport_bytes_total_.Set(network_transport_total());

        /* collect interval */
        // TODO: interval from config
        sleep(1);
    }
}

double
SystemInfoCollector::cpu_utilization_ratio() {
    clock_t now_cpu, now_sys_cpu, now_user_cpu;
    SystemInfo::CpuUtilizationRatio(now_cpu, now_sys_cpu, now_user_cpu);

    double value = 0;
    if (now_cpu <= base_cpu_ || now_sys_cpu < base_sys_cpu_ || now_user_cpu < base_user_cpu_) {
        // Overflow detection. Just skip this value.
        value = -1.0;
    } else {
        value = (now_sys_cpu - base_sys_cpu_) + (now_user_cpu - base_user_cpu_);
        value /= (now_cpu - base_cpu_);
        value *= 100;
    }
    base_cpu_ = now_cpu;
    base_sys_cpu_ = now_sys_cpu;
    base_user_cpu_ = now_user_cpu;
    if (0 <= value && value <= 256 * 100) {
        return value;
    } else {
        return nan("1");
    }
}

double
SystemInfoCollector::cpu_temperature_celsius() {
    auto value = SystemInfo::CpuTemperature();
    if (-40 <= value && value <= 120) {
        return value;
    } else {
        return nan("1");
    }
}

double
SystemInfoCollector::mem_usage() {
    auto value = SystemInfo::MemUsage();
    if (0 <= value && value <= engine::TB) {
        return value;
    } else {
        return nan("1");
    }
}

double
SystemInfoCollector::mem_available() {
    auto value = SystemInfo::MemAvailable();
    if (0 <= value && value <= engine::TB) {
        return value;
    } else {
        return nan("1");
    }
}

double
SystemInfoCollector::network_receive_total() {
    auto value = SystemInfo::NetworkInOctets() - base_network_in_octets_;
    if (0 <= value && value <= engine::GB) {
        return value;
    } else {
        return nan("1");
    }
}

double
SystemInfoCollector::network_transport_total() {
    auto value = SystemInfo::NetworkOutOctets() - base_network_out_octets_;
    if (0 <= value && value <= engine::GB) {
        return value;
    } else {
        return nan("1");
    }
}

}  // namespace milvus
