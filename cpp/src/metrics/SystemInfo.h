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

#include "sys/types.h"
#include "sys/sysinfo.h"
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/times.h"
#include "sys/vtimes.h"
#include <chrono>

#include <unordered_map>
#include <vector>



namespace zilliz {
namespace milvus {
namespace server {

class SystemInfo {
 private:
    unsigned long total_ram_ = 0;
    clock_t last_cpu_ = clock_t();
    clock_t last_sys_cpu_ = clock_t();
    clock_t last_user_cpu_ = clock_t();
    std::chrono::system_clock::time_point net_time_ = std::chrono::system_clock::now();
    int num_processors_ = 0;
    int num_physical_processors_ = 0;
    //number of GPU
    unsigned int num_device_ = 0;
    unsigned long long in_octets_ = 0;
    unsigned long long out_octets_ = 0;
    bool initialized_ = false;

 public:
    static SystemInfo &
    GetInstance(){
        static SystemInfo instance;
        return instance;
    }

    void Init();
    int num_processor() const { return num_processors_;};
    int num_physical_processors() const { return num_physical_processors_; };
    int num_device() const {return num_device_;};
    unsigned long long get_inoctets() { return in_octets_;};
    unsigned long long get_octets() { return out_octets_;};
    std::chrono::system_clock::time_point get_nettime() { return net_time_;};
    void set_inoctets(unsigned long long value) { in_octets_ = value;};
    void set_outoctets(unsigned long long value) { out_octets_ = value;};
    void set_nettime() {net_time_ = std::chrono::system_clock::now();};
    long long ParseLine(char* line);
    unsigned long GetPhysicalMemory();
    unsigned long GetProcessUsedMemory();
    double MemoryPercent();
    double CPUPercent();
    std::pair<unsigned long long , unsigned long long > Octets();
    std::vector<unsigned long long> GPUMemoryTotal();
    std::vector<unsigned long long> GPUMemoryUsed();

    std::vector<double> CPUCorePercent();
    std::vector<unsigned long long> getTotalCpuTime(std::vector<unsigned long long> &workTime);
    std::vector<unsigned int> GPUTemperature();
    std::vector<float> CPUTemperature();

};

}
}
}
