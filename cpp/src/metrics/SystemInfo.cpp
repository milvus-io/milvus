/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "SystemInfo.h"

#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include "nvml.h"
//#include <mutex>
//
//std::mutex mutex;


namespace zilliz {
namespace vecwise {
namespace server {

void SystemInfo::Init() {
    if(initialized) return;
//    mutex.lock();
    initialized = true;
//    mutex.unlock();
    // initialize CPU information
    FILE* file;
    struct tms timeSample;
    char line[128];
    lastCPU_ = times(&timeSample);
    lastSysCPU_ = timeSample.tms_stime;
    lastUserCPU_ = timeSample.tms_utime;
    file = fopen("/proc/cpuinfo", "r");
    numProcessors = 0;
    while(fgets(line, 128, file) != NULL){
        if (strncmp(line, "processor", 9) == 0) numProcessors++;
    }
    total_RAM_ = GetPhysicalMemory();
    fclose(file);

    //initialize GPU information
    nvmlReturn_t nvmlresult;
    nvmlresult = nvmlInit();
    if(NVML_SUCCESS != nvmlresult) {
        printf("System information initilization failed");
        return ;
    }
    nvmlresult = nvmlDeviceGetCount(&numDevice);
    if(NVML_SUCCESS != nvmlresult) {
        printf("Unable to get devidce number");
        return ;
    }

}

long long
SystemInfo::parseLine(char *line) {
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9') p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return static_cast<long long>(i);
}

unsigned long
SystemInfo::GetPhysicalMemory() {
    struct sysinfo memInfo;
    sysinfo (&memInfo);
    unsigned long totalPhysMem = memInfo.totalram;
    //Multiply in next statement to avoid int overflow on right hand side...
    totalPhysMem *= memInfo.mem_unit;
    return totalPhysMem;
}

unsigned long
SystemInfo::GetProcessUsedMemory() {
    //Note: this value is in KB!
    FILE* file = fopen("/proc/self/status", "r");
    long long result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
//    printf("RAM is %d",result);
    // return value in Byte
    return (result*1024);

}

double
SystemInfo::MemoryPercent() {
    if (!initialized) Init();
    return GetProcessUsedMemory()*100/total_RAM_;
}

double
SystemInfo::CPUPercent() {
    if (!initialized) Init();
    struct tms timeSample;
    clock_t now;
    double percent;

    now = times(&timeSample);
    if (now <= lastCPU_ || timeSample.tms_stime < lastSysCPU_ ||
        timeSample.tms_utime < lastUserCPU_){
        //Overflow detection. Just skip this value.
        percent = -1.0;
    }
    else{
        percent = (timeSample.tms_stime - lastSysCPU_) +
            (timeSample.tms_utime - lastUserCPU_);
        percent /= (now - lastCPU_);
        percent /= numProcessors;
        percent *= 100;
    }
    lastCPU_ = now;
    lastSysCPU_ = timeSample.tms_stime;
    lastUserCPU_ = timeSample.tms_utime;

    return percent;
}

std::unordered_map<int,std::vector<double>>
SystemInfo::GetGPUMemPercent(){
    // return GPUID: MEM%

    //write GPU info to a file
    system("nvidia-smi pmon -c 1 > GPUInfo.txt");
    int pid = (int)getpid();

    //parse line
    std::ifstream read_file;
    read_file.open("GPUInfo.txt");
    std::string line;
    while(getline(read_file, line)){
        std::vector<std::string> words = split(line);
        //                    0      1     2    3   4    5    6      7
        //words stand for gpuindex, pid, type, sm, mem, enc, dec, command respectively
        if(std::stoi(words[1]) != pid) continue;
        int GPUindex = std::stoi(words[0]);
        double sm_percent = std::stod(words[3]);
        double mem_percent = std::stod(words[4]);

    }

}

std::vector<std::string>
SystemInfo::split(std::string input) {
    std::vector<std::string> words;
    input += " ";
    int word_start = 0;
    for (int i = 0; i < input.size(); ++i) {
        if(input[i] != ' ') continue;
        if(input[i] == ' ') {
            word_start = i + 1;
            continue;
        }
        words.push_back(input.substr(word_start,i-word_start));
    }
    return words;
}

std::vector<unsigned int>
SystemInfo::GPUPercent() {
    // get GPU usage percent
    if(!initialized) Init();
    std::vector<unsigned int> result;
    nvmlUtilization_t utilization;
    for (int i = 0; i < numDevice; ++i) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        nvmlDeviceGetUtilizationRates(device, &utilization);
        result.push_back(utilization.gpu);
    }
    return result;
//        nvmlDevice_t device;
//        nvmlUtilization_t utilization;
//        nvmlDeviceGetHandleByIndex(device_index, &device);
//        nvmlDeviceGetUtilizationRates(device, &utilization);
//        return utilization.gpu;
}

std::vector<unsigned long long>
SystemInfo::GPUMemoryUsed() {
    // get GPU memory used
    if(!initialized) Init();

    std::vector<unsigned long long int> result;
    nvmlMemory_t nvmlMemory;
    for (int i = 0; i < numDevice; ++i) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        result.push_back(nvmlMemory.used);
    }
    return result;
}

}
}
}