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
    if (running_)
        return;
    running_ = true;
    collector_thread_ = std::thread(&SystemInfoCollector::collector_function, this);
}

void
SystemInfoCollector::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (not running_)
        return;
    running_ = false;
    collector_thread_.join();
}

void
SystemInfoCollector::collector_function() {
    auto network_in = SystemInfo::NetworkInOctets();
    auto network_out = SystemInfo::NetworkOutOctets();
    while (running_) {
        /* collect metrics */
        // cpu_utilization_ratio range: 0~2560000%
        cpu_utilization_ratio_.Set(cpu_utilization_ratio());
        // cpu_temperature range: -40°C~120°C
        cpu_temperature_.Set(cpu_temperature_celsius());
        // mem_usage range: 0~1TB
        mem_usage_.Set(mem_usage());
        // mem_available range: 0~1TB
        mem_available_.Set(mem_available());
        // network_in_octets range: 0~1GB
        network_in_octets_.Set(network_receive_speed());
        // network_out_octets range: 0~1GB
        network_out_octets_.Set(network_transport_speed());

        /* collect interval */
        // TODO: interval from config
        sleep(1);
    }
}

double
SystemInfoCollector::cpu_utilization_ratio() {
    auto value = SystemInfo::CpuUtilizationRatio();
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
SystemInfoCollector::network_receive_speed() {
    auto network_in_octets = SystemInfo::NetworkInOctets();
    auto value = network_in_octets - last_network_in_octets_;
    last_network_in_octets_ = network_in_octets;
    if (0 <= value && value <= engine::GB) {
        return value;
    } else {
        return nan("1");
    }
}

double
SystemInfoCollector::network_transport_speed() {
    auto network_out_octets = SystemInfo::NetworkOutOctets();
    auto value = network_out_octets - last_network_out_octets_;
    last_network_out_octets_ = network_out_octets;
    if (0 <= value && value <= engine::GB) {
        return value;
    } else {
        return nan("1");
    }
}

}  // namespace milvus
