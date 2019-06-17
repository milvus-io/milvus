/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

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
//    std::unordered_map<int,std::vector<double>> GetGPUMemPercent() {};
//    std::vector<std::string> split(std::string input) {};
    std::vector<unsigned int> GPUPercent();
    std::vector<unsigned long long> GPUMemoryUsed();

};

}
}
}
