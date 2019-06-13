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
    int num_processors_ = 0;
    //number of GPU
    unsigned int num_device_ = 0;
    bool initialized_ = false;

 public:
    static SystemInfo &
    GetInstance(){
        static SystemInfo instance;
        return instance;
    }

    void Init();
    int num_device() const {return num_device_;};
    long long ParseLine(char* line);
    unsigned long GetPhysicalMemory();
    unsigned long GetProcessUsedMemory();
    double MemoryPercent();
    double CPUPercent();
//    std::unordered_map<int,std::vector<double>> GetGPUMemPercent() {};
//    std::vector<std::string> split(std::string input) {};
    std::vector<unsigned int> GPUPercent();
    std::vector<unsigned long long> GPUMemoryUsed();

};

}
}
}
