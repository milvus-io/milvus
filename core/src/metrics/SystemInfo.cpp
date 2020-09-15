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

#include "metrics/SystemInfo.h"
#include "thirdparty/nlohmann/json.hpp"
#include "utils/Exception.h"
#include "utils/Log.h"

#include <dirent.h>
#include <fiu/fiu-local.h>
#include <sys/sysinfo.h>
#include <sys/times.h>
#include <unistd.h>
#include <map>

#ifdef MILVUS_GPU_VERSION

#include <nvml.h>

#endif

namespace milvus {
namespace server {

void
SystemInfo::Init() {
    if (initialized_) {
        return;
    }

    initialized_ = true;

    // initialize CPU information
    try {
        struct tms time_sample;
        last_cpu_ = times(&time_sample);
        last_sys_cpu_ = time_sample.tms_stime;
        last_user_cpu_ = time_sample.tms_utime;
        num_processors_ = 0;
        FILE* file = fopen("/proc/cpuinfo", "r");
        if (file) {
            char line[128];
            while (fgets(line, 128, file) != nullptr) {
                if (strncmp(line, "processor", 9) == 0) {
                    num_processors_++;
                }
                if (strncmp(line, "physical", 8) == 0) {
                    num_physical_processors_ = ParseLine(line);
                }
            }
            fclose(file);
        } else {
            LOG_SERVER_ERROR_ << "Failed to read /proc/cpuinfo";
        }
        total_ram_ = GetPhysicalMemory();
    } catch (std::exception& ex) {
        std::string msg = "Failed to read /proc/cpuinfo, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
    }

#ifdef MILVUS_GPU_VERSION
    // initialize GPU information
    nvmlReturn_t nvmlresult;
    nvmlresult = nvmlInit();
    fiu_do_on("SystemInfo.Init.nvmInit_fail", nvmlresult = NVML_ERROR_NOT_FOUND);
    if (NVML_SUCCESS != nvmlresult) {
        LOG_SERVER_ERROR_ << "System information initilization failed";
        return;
    }
    nvmlresult = nvmlDeviceGetCount(&num_device_);
    fiu_do_on("SystemInfo.Init.nvm_getDevice_fail", nvmlresult = NVML_ERROR_NOT_FOUND);
    if (NVML_SUCCESS != nvmlresult) {
        LOG_SERVER_ERROR_ << "Unable to get devidce number";
        return;
    }
#endif

    // initialize network traffic information
    try {
        std::pair<int64_t, int64_t> in_and_out_octets = Octets();
        in_octets_ = in_and_out_octets.first;
        out_octets_ = in_and_out_octets.second;
        net_time_ = std::chrono::system_clock::now();
    } catch (std::exception& ex) {
        std::string msg = "Failed to initialize network traffic information, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
    }
}

int64_t
SystemInfo::ParseLine(char* line) {
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p < '0' || *p > '9') {
        p++;
    }
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}

int64_t
SystemInfo::GetPhysicalMemory() {
    struct sysinfo memInfo;
    sysinfo(&memInfo);
    int64_t totalPhysMem = memInfo.totalram;
    // Multiply in next statement to avoid int overflow on right hand side...
    totalPhysMem *= memInfo.mem_unit;

    return totalPhysMem;
}

int64_t
SystemInfo::GetProcessUsedMemory() {
    try {
        // Note: this value is in KB!
        FILE* file = fopen("/proc/self/status", "r");
        int64_t result = 0;
        constexpr int64_t KB = 1024;
        if (file) {
            constexpr int64_t line_length = 128;
            char line[line_length];

            while (fgets(line, line_length, file) != nullptr) {
                if (strncmp(line, "VmRSS:", 6) == 0) {
                    result = ParseLine(line);
                    break;
                }
            }
            fclose(file);
        } else {
            LOG_SERVER_ERROR_ << "Failed to read /proc/self/status";
        }

        // return value in Byte
        return (result * KB);
    } catch (std::exception& ex) {
        std::string msg = "Failed to read /proc/self/status, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
        return 0;
    }
}

double
SystemInfo::MemoryPercent() {
    fiu_do_on("SystemInfo.MemoryPercent.mock", initialized_ = false);
    if (!initialized_) {
        Init();
    }

    auto mem_used = static_cast<double>(GetProcessUsedMemory() * 100);
    return mem_used / static_cast<double>(total_ram_);
}

std::vector<double>
SystemInfo::CPUCorePercent() {
    std::vector<int64_t> prev_work_time_array;
    std::vector<int64_t> prev_total_time_array = getTotalCpuTime(prev_work_time_array);
    usleep(100000);
    std::vector<int64_t> cur_work_time_array;
    std::vector<int64_t> cur_total_time_array = getTotalCpuTime(cur_work_time_array);

    std::vector<double> cpu_core_percent;
    for (size_t i = 0; i < cur_total_time_array.size(); i++) {
        double total_cpu_time = cur_total_time_array[i] - prev_total_time_array[i];
        double cpu_work_time = cur_work_time_array[i] - prev_work_time_array[i];
        cpu_core_percent.push_back((cpu_work_time / total_cpu_time) * 100);
    }
    return cpu_core_percent;
}

std::vector<int64_t>
SystemInfo::getTotalCpuTime(std::vector<int64_t>& work_time_array) {
    std::vector<int64_t> total_time_array;
    try {
        FILE* file = fopen("/proc/stat", "r");
        fiu_do_on("SystemInfo.getTotalCpuTime.open_proc", file = nullptr);
        if (file == nullptr) {
            LOG_SERVER_ERROR_ << "Failed to read /proc/stat";
            return total_time_array;
        }

        int64_t user = 0, nice = 0, system = 0, idle = 0;
        int64_t iowait = 0, irq = 0, softirq = 0, steal = 0, guest = 0, guestnice = 0;

        for (int i = 0; i < num_processors_; i++) {
            char buffer[1024];
            char* ret = fgets(buffer, sizeof(buffer) - 1, file);
            fiu_do_on("SystemInfo.getTotalCpuTime.read_proc", ret = nullptr);
            if (ret == nullptr) {
                LOG_SERVER_ERROR_ << "Could not read stat file";
                fclose(file);
                return total_time_array;
            }

            sscanf(buffer, "cpu  %16ld %16ld %16ld %16ld %16ld %16ld %16ld %16ld %16ld %16ld", &user, &nice, &system,
                   &idle, &iowait, &irq, &softirq, &steal, &guest, &guestnice);

            work_time_array.push_back(user + nice + system);
            total_time_array.push_back(user + nice + system + idle + iowait + irq + softirq + steal);
        }

        fclose(file);
    } catch (std::exception& ex) {
        std::string msg = "Failed to read /proc/stat, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
    }

    return total_time_array;
}

double
SystemInfo::CPUPercent() {
    fiu_do_on("SystemInfo.CPUPercent.mock", initialized_ = false);
    if (!initialized_) {
        Init();
    }
    struct tms time_sample;
    clock_t now;
    double percent;

    now = times(&time_sample);
    if (now <= last_cpu_ || time_sample.tms_stime < last_sys_cpu_ || time_sample.tms_utime < last_user_cpu_) {
        // Overflow detection. Just skip this value.
        percent = -1.0;
    } else {
        percent = (time_sample.tms_stime - last_sys_cpu_) + (time_sample.tms_utime - last_user_cpu_);
        percent /= (now - last_cpu_);
        percent *= 100;
    }
    last_cpu_ = now;
    last_sys_cpu_ = time_sample.tms_stime;
    last_user_cpu_ = time_sample.tms_utime;

    return percent;
}

std::vector<int64_t>
SystemInfo::GPUMemoryTotal() {
    // get GPU usage percent
    fiu_do_on("SystemInfo.GPUMemoryTotal.mock", initialized_ = false);
    if (!initialized_) {
        Init();
    }
    std::vector<int64_t> result;

#ifdef MILVUS_GPU_VERSION
    nvmlMemory_t nvmlMemory;
    for (uint32_t i = 0; i < num_device_; ++i) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        result.push_back(nvmlMemory.total);
    }
#endif

    return result;
}

std::vector<int64_t>
SystemInfo::GPUTemperature() {
    fiu_do_on("SystemInfo.GPUTemperature.mock", initialized_ = false);
    if (!initialized_) {
        Init();
    }
    std::vector<int64_t> result;

#ifdef MILVUS_GPU_VERSION
    for (uint32_t i = 0; i < num_device_; i++) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        unsigned int temp;
        nvmlDeviceGetTemperature(device, NVML_TEMPERATURE_GPU, &temp);
        result.push_back(temp);
    }
#endif

    return result;
}

std::vector<float>
SystemInfo::CPUTemperature() {
    std::vector<float> result;
    std::string path = "/sys/class/hwmon/";
    try {
        DIR* dir = opendir(path.c_str());
        fiu_do_on("SystemInfo.CPUTemperature.opendir", dir = nullptr);
        if (!dir) {
            LOG_SERVER_ERROR_ << "Could not open hwmon directory";
            return result;
        }

        struct dirent* ptr = nullptr;
        while ((ptr = readdir(dir)) != nullptr) {
            std::string filename(path);
            filename.append(ptr->d_name);

            char buf[100];
            if (readlink(filename.c_str(), buf, 100) != -1) {
                std::string m(buf);
                if (m.find("coretemp") != std::string::npos) {
                    std::string object = filename;
                    object += "/temp1_input";
                    FILE* file = fopen(object.c_str(), "r");
                    fiu_do_on("SystemInfo.CPUTemperature.openfile", file = nullptr);
                    if (file == nullptr) {
                        LOG_SERVER_ERROR_ << "Could not open temperature file";
                        return result;
                    }
                    float temp;
                    if (fscanf(file, "%f", &temp) != -1) {
                        result.push_back(temp / 1000);
                    }
                    fclose(file);
                }
            }
        }
        closedir(dir);
    } catch (std::exception& ex) {
        std::string msg = "Failed to get cpu temperature, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
    }

    return result;
}

std::vector<int64_t>
SystemInfo::GPUMemoryUsed() {
    // get GPU memory used
    fiu_do_on("SystemInfo.GPUMemoryUsed.mock", initialized_ = false);
    if (!initialized_) {
        Init();
    }

    std::vector<int64_t> result;

#ifdef MILVUS_GPU_VERSION
    nvmlMemory_t nvmlMemory;
    for (uint32_t i = 0; i < num_device_; ++i) {
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(i, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        result.push_back(nvmlMemory.used);
    }
#endif

    return result;
}

std::pair<int64_t, int64_t>
SystemInfo::Octets() {
    const std::string filename = "/proc/net/netstat";
    try {
        std::ifstream file(filename);
        std::string lastline = "";
        std::string line = "";
        while (true) {
            getline(file, line);
            if (file.fail()) {
                break;
            }
            lastline = line;
        }
        std::vector<size_t> space_position;
        size_t space_pos = lastline.find(' ');
        while (space_pos != std::string::npos) {
            space_position.push_back(space_pos);
            space_pos = lastline.find(' ', space_pos + 1);
        }
        // InOctets is between 6th and 7th " " and OutOctets is between 7th and 8th " "
        if (space_position.size() < 9) {
            std::string err = "space_position size(" + std::to_string(space_position.size()) + ") < 9";
            throw std::runtime_error(err);
        }

        size_t inoctets_begin = space_position[6] + 1;
        size_t inoctets_length = space_position[7] - inoctets_begin;
        size_t outoctets_begin = space_position[7] + 1;
        size_t outoctets_length = space_position[8] - outoctets_begin;
        std::string inoctets = lastline.substr(inoctets_begin, inoctets_length);
        std::string outoctets = lastline.substr(outoctets_begin, outoctets_length);

        int64_t inoctets_bytes = std::stoull(inoctets);
        int64_t outoctets_bytes = std::stoull(outoctets);
        std::pair<int64_t, int64_t> res(inoctets_bytes, outoctets_bytes);
        return res;
    } catch (std::exception& e) {
        LOG_SERVER_ERROR_ << "failed to read file " << filename << ": " << e.what();
        return std::pair<int64_t, int64_t>(0, 0);
    } catch (...) {
        LOG_SERVER_ERROR_ << "failed to read file " << filename << ": Unknown exception";
        return std::pair<int64_t, int64_t>(0, 0);
    }
}

void
SystemInfo::GetSysInfoJsonStr(std::string& result) {
    std::map<std::string, std::string> sys_info_map;

    sys_info_map["memory_total"] = std::to_string(GetPhysicalMemory());
    sys_info_map["memory_used"] = std::to_string(GetProcessUsedMemory());

    std::vector<int64_t> gpu_mem_total = GPUMemoryTotal();
    std::vector<int64_t> gpu_mem_used = GPUMemoryUsed();
    for (size_t i = 0; i < gpu_mem_total.size(); i++) {
        std::string key_total = "gpu" + std::to_string(i) + "_memory_total";
        std::string key_used = "gpu" + std::to_string(i) + "_memory_used";
        sys_info_map[key_total] = std::to_string(gpu_mem_total[i]);
        sys_info_map[key_used] = std::to_string(gpu_mem_used[i]);
    }

    nlohmann::json sys_info_json(sys_info_map);
    result = sys_info_json.dump();
}

}  // namespace server
}  // namespace milvus
