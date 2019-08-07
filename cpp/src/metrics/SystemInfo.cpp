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
namespace milvus {
namespace server {

void SystemInfo::Init() {
    if(initialized_) return;

    initialized_ = true;

    // initialize CPU information
    FILE* file;
    struct tms time_sample;
    char line[128];
    last_cpu_ = times(&time_sample);
    last_sys_cpu_ = time_sample.tms_stime;
    last_user_cpu_ = time_sample.tms_utime;
    file = fopen("/proc/cpuinfo", "r");
    num_processors_ = 0;
    while(fgets(line, 128, file) != NULL){
        if (strncmp(line, "processor", 9) == 0) num_processors_++;
        if (strncmp(line, "physical", 8) == 0) {
            num_physical_processors_ = ParseLine(line);
        }
    }
    total_ram_ = GetPhysicalMemory();
    fclose(file);

    //initialize GPU information
    nvmlReturn_t nvmlresult;
    nvmlresult = nvmlInit();
    if(NVML_SUCCESS != nvmlresult) {
        printf("System information initilization failed");
        return ;
    }
    nvmlresult = nvmlDeviceGetCount(&num_device_);
    if(NVML_SUCCESS != nvmlresult) {
        printf("Unable to get devidce number");
        return ;
    }

    //initialize network traffic information
    std::pair<unsigned long long, unsigned long long> in_and_out_octets = Octets();
    in_octets_ = in_and_out_octets.first;
    out_octets_ = in_and_out_octets.second;
    net_time_ = std::chrono::system_clock::now();
}

long long
SystemInfo::ParseLine(char *line) {
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
    constexpr int64_t line_length = 128;
    long long result = -1;
    constexpr int64_t KB_SIZE = 1024;
    char line[line_length];

    while (fgets(line, line_length, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = ParseLine(line);
            break;
        }
    }
    fclose(file);
    // return value in Byte
    return (result*KB_SIZE);

}

double
SystemInfo::MemoryPercent() {
    if (!initialized_) Init();
    return (double)(GetProcessUsedMemory()*100)/(double)total_ram_;
}

std::vector<double>
SystemInfo::CPUCorePercent() {
    std::vector<unsigned long long> prev_work_time_array;
    std::vector<unsigned long long> prev_total_time_array = getTotalCpuTime(prev_work_time_array);
    usleep(100000);
    std::vector<unsigned long long> cur_work_time_array;
    std::vector<unsigned long long> cur_total_time_array = getTotalCpuTime(cur_work_time_array);

    std::vector<double> cpu_core_percent;
    for (int i = 1; i < num_processors_; i++) {
        double total_cpu_time = cur_total_time_array[i] - prev_total_time_array[i];
        double cpu_work_time = cur_work_time_array[i] - prev_work_time_array[i];
        cpu_core_percent.push_back((cpu_work_time / total_cpu_time) * 100);
    }
    return cpu_core_percent;
}

std::vector<unsigned long long>
SystemInfo::getTotalCpuTime(std::vector<unsigned long long> &work_time_array)
{
    std::vector<unsigned long long> total_time_array;
    FILE* file = fopen("/proc/stat", "r");
    if (file == NULL) {
        perror("Could not open stat file");
        return total_time_array;
    }

    unsigned long long user = 0, nice = 0, system = 0, idle = 0;
    unsigned long long iowait = 0, irq = 0, softirq = 0, steal = 0, guest = 0, guestnice = 0;

    for (int i = 0; i < num_processors_; i++) {
        char buffer[1024];
        char* ret = fgets(buffer, sizeof(buffer) - 1, file);
        if (ret == NULL) {
            perror("Could not read stat file");
            fclose(file);
            return total_time_array;
        }

        sscanf(buffer,
               "cpu  %16llu %16llu %16llu %16llu %16llu %16llu %16llu %16llu %16llu %16llu",
               &user, &nice, &system, &idle, &iowait, &irq, &softirq, &steal, &guest, &guestnice);

        work_time_array.push_back(user + nice + system);
        total_time_array.push_back(user + nice + system + idle + iowait + irq + softirq + steal);
    }

    fclose(file);
    return total_time_array;
}




double
SystemInfo::CPUPercent() {
    if (!initialized_) Init();
    struct tms time_sample;
    clock_t now;
    double percent;

    now = times(&time_sample);
    if (now <= last_cpu_ || time_sample.tms_stime < last_sys_cpu_ ||
        time_sample.tms_utime < last_user_cpu_){
        //Overflow detection. Just skip this value.
        percent = -1.0;
    }
    else{
        percent = (time_sample.tms_stime - last_sys_cpu_) +
            (time_sample.tms_utime - last_user_cpu_);
        percent /= (now - last_cpu_);
        percent *= 100;
    }
    last_cpu_ = now;
    last_sys_cpu_ = time_sample.tms_stime;
    last_user_cpu_ = time_sample.tms_utime;

    return percent;
}


std::vector<unsigned long long>
SystemInfo::GPUMemoryTotal() {
    // get GPU usage percent
    if(!initialized_) Init();
    std::vector<unsigned long long > result;
    nvmlMemory_t nvmlMemory;
    for (int i = 0; i < num_device_; ++i) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        result.push_back(nvmlMemory.total);
    }
    return result;
}

std::vector<unsigned int>
SystemInfo::GPUTemperature(){
    if(!initialized_) Init();
    std::vector<unsigned int > result;
    for (int i = 0; i < num_device_; i++) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        unsigned int temp;
        nvmlDeviceGetTemperature(device, NVML_TEMPERATURE_GPU,&temp);
        result.push_back(temp);
    }
    return result;
}
std::vector<float>
SystemInfo::CPUTemperature(){
    std::vector<float> result;
    for (int i = 0; i <= num_physical_processors_; ++i) {
        std::string path = "/sys/class/thermal/thermal_zone" + std::to_string(i) + "/temp";
        FILE *file = fopen(path.data(), "r");
        if (file == NULL) {
            perror("Could not open thermal file");
            return result;
        }
        float temp;
        fscanf(file, "%f", &temp);
        result.push_back(temp / 1000);
    }

}

std::vector<unsigned long long>
SystemInfo::GPUMemoryUsed() {
    // get GPU memory used
    if(!initialized_) Init();

    std::vector<unsigned long long int> result;
    nvmlMemory_t nvmlMemory;
    for (int i = 0; i < num_device_; ++i) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        result.push_back(nvmlMemory.used);
    }
    return result;
}

std::pair<unsigned long long , unsigned long long >
SystemInfo::Octets(){
    pid_t pid = getpid();
//    const std::string filename = "/proc/"+std::to_string(pid)+"/net/netstat";
    const std::string filename = "/proc/net/netstat";
    std::ifstream file(filename);
    std::string lastline = "";
    std::string line = "";
    while(file){
        getline(file, line);
        if(file.fail()){
            break;
        }
        lastline = line;
    }
    std::vector<size_t> space_position;
    size_t space_pos = lastline.find(" ");
    while(space_pos != std::string::npos){
        space_position.push_back(space_pos);
        space_pos = lastline.find(" ",space_pos+1);
    }
    // InOctets is between 6th and 7th " " and OutOctets is between 7th and 8th " "
    size_t inoctets_begin = space_position[6]+1;
    size_t inoctets_length = space_position[7]-inoctets_begin;
    size_t outoctets_begin = space_position[7]+1;
    size_t outoctets_length = space_position[8]-outoctets_begin;
    std::string inoctets = lastline.substr(inoctets_begin,inoctets_length);
    std::string outoctets = lastline.substr(outoctets_begin,outoctets_length);


    unsigned long long inoctets_bytes = std::stoull(inoctets);
    unsigned long long outoctets_bytes = std::stoull(outoctets);
    std::pair<unsigned long long , unsigned long long > res(inoctets_bytes, outoctets_bytes);
    return res;
}

}
}
}