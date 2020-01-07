// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <chrono>
#include <string>
#include <utility>
#include <vector>

namespace milvus {
namespace server {

class SystemInfo {
 private:
    uint64_t total_ram_ = 0;
    clock_t last_cpu_ = clock_t();
    clock_t last_sys_cpu_ = clock_t();
    clock_t last_user_cpu_ = clock_t();
    std::chrono::system_clock::time_point net_time_ = std::chrono::system_clock::now();
    int num_processors_ = 0;
    int num_physical_processors_ = 0;
    // number of GPU
    uint32_t num_device_ = 0;
    uint64_t in_octets_ = 0;
    uint64_t out_octets_ = 0;
    bool initialized_ = false;

 public:
    static SystemInfo&
    GetInstance() {
        static SystemInfo instance;
        return instance;
    }

    void
    Init();

    int
    num_processor() const {
        return num_processors_;
    }

    int
    num_physical_processors() const {
        return num_physical_processors_;
    }

    uint32_t
    num_device() const {
        return num_device_;
    }

    uint64_t
    get_inoctets() {
        return in_octets_;
    }

    uint64_t
    get_octets() {
        return out_octets_;
    }

    std::chrono::system_clock::time_point
    get_nettime() {
        return net_time_;
    }

    void
    set_inoctets(uint64_t value) {
        in_octets_ = value;
    }

    void
    set_outoctets(uint64_t value) {
        out_octets_ = value;
    }

    void
    set_nettime() {
        net_time_ = std::chrono::system_clock::now();
    }

    uint64_t
    ParseLine(char* line);
    uint64_t
    GetPhysicalMemory();
    uint64_t
    GetProcessUsedMemory();
    double
    MemoryPercent();
    double
    CPUPercent();
    std::pair<uint64_t, uint64_t>
    Octets();
    std::vector<uint64_t>
    GPUMemoryTotal();
    std::vector<uint64_t>
    GPUMemoryUsed();

    std::vector<double>
    CPUCorePercent();
    std::vector<uint64_t>
    getTotalCpuTime(std::vector<uint64_t>& workTime);
    std::vector<uint64_t>
    GPUTemperature();
    std::vector<float>
    CPUTemperature();

    void
    GetSysInfoJsonStr(std::string& result);
};

}  // namespace server
}  // namespace milvus
