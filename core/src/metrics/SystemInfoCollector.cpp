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
    while (running_) {
        /* collect metrics */
        cpu_utilization_ratio_.Set(SystemInfo::CpuUtilizationRatio());
        auto temperature = SystemInfo::CpuTemperature();
        cpu_temperature_.Set((temperature < -40 || temperature > 120) ? nan("1") : temperature);
        mem_usage_.Set(SystemInfo::MemUsage());
        mem_total_.Set(SystemInfo::MemTotal());
        mem_available_.Set(SystemInfo::MemAvailable());
        network_in_octets_.Set(SystemInfo::NetworkInOctets());
        network_out_octets_.Set(SystemInfo::NetworkOutOctets());

        /* collect interval */
        // TODO: interval from config
        sleep(1);
    }
}

}  // namespace milvus
