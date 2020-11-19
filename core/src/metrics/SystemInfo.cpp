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
#include <dirent.h>
#include <sys/sysinfo.h>
#include <sys/times.h>
#include <unistd.h>
#include <map>
#include "utils/Exception.h"
#include "utils/Log.h"

#ifdef MILVUS_GPU_VERSION

#include <nvml.h>

#endif

namespace milvus {

double
SystemInfo::CpuUtilizationRatio() {
    double percent = 0;
    try {
        struct tms time_first {};
        clock_t last_cpu_ = times(&time_first);
        clock_t last_sys_cpu_ = time_first.tms_stime;
        clock_t last_user_cpu_ = time_first.tms_utime;
        usleep(100000);

        struct tms time_sample {};
        clock_t now;

        now = times(&time_sample);
        if (now <= last_cpu_ || time_sample.tms_stime < last_sys_cpu_ || time_sample.tms_utime < last_user_cpu_) {
            // Overflow detection. Just skip this value.
            percent = -1.0;
        } else {
            percent = (time_sample.tms_stime - last_sys_cpu_) + (time_sample.tms_utime - last_user_cpu_);
            percent /= (now - last_cpu_);
            percent *= 100;
        }
    } catch (std::exception& ex) {
        std::string msg = "Cannot get cpu utilization ratio, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }
    return percent;
}

int64_t
SystemInfo::CpuTemperature() {
    int64_t temperature = 0;
    try {
        std::vector<float> cpu_temperatures;
        std::string path = "/sys/class/hwmon/";
        DIR* dir = opendir(path.c_str());
        if (!dir) {
            std::string msg = "Could not open hwmon directory";
            throw std::runtime_error(msg);
        }

        struct dirent* ptr = NULL;
        while ((ptr = readdir(dir)) != NULL) {
            std::string filename(path);
            filename.append(ptr->d_name);

            char buf[100];
            if (readlink(filename.c_str(), buf, 100) != -1) {
                std::string m(buf);
                if (m.find("coretemp") != std::string::npos) {
                    std::string object = filename;
                    object += "/temp1_input";
                    FILE* file = fopen(object.c_str(), "r");
                    if (file == nullptr) {
                        std::string msg = "Cannot open file " + object;
                        throw std::runtime_error(msg);
                    }
                    float temp;
                    if (fscanf(file, "%f", &temp) != -1) {
                        cpu_temperatures.push_back(temp / 1000);
                    }
                    fclose(file);
                }
            }
        }
        closedir(dir);

        float avg_cpu_temp = 0;
        for (float cpu_temperature : cpu_temperatures) {
            avg_cpu_temp += cpu_temperature;
        }
        temperature = avg_cpu_temp / cpu_temperatures.size();
    } catch (std::exception& ex) {
        std::string msg = "Cannot get cpu temperature, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }

    return temperature;
}

int64_t
SystemInfo::MemUsage() {
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
            std::string msg = "Failed to read /proc/self/status";
            throw std::runtime_error(msg);
        }

        // return value in Byte
        return (result * KB);
    } catch (std::exception& ex) {
        std::string msg = "Cannot get cpu memory usage, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::MemTotal() {
    int64_t total_physMem;
    try {
        struct sysinfo memInfo;
        sysinfo(&memInfo);
        total_physMem = memInfo.totalram;
        // Multiply in next statement to avoid int overflow on right hand side...
        total_physMem *= memInfo.mem_unit;
    } catch (std::exception& ex) {
        std::string msg = "Cannot get total memory, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }

    return total_physMem;
}

int64_t
SystemInfo::MemAvailable() {
    try {
        return MemTotal() - MemUsage();
    } catch (std::exception& ex) {
        std::string msg = "Cannot get total memory, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::NetworkInOctets() {
    try {
        auto in_and_out_octets = Octets();
        return in_and_out_octets.first;
    } catch (std::exception& ex) {
        std::string msg = "Cannot get network in octets, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::NetworkOutOctets() {
    try {
        auto in_and_out_octets = Octets();
        return in_and_out_octets.second;
    } catch (std::exception& ex) {
        std::string msg = "Cannot get network out octets, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }
}

double
SystemInfo::FloatingReadSpeed() {
}

#ifdef MILVUS_GPU_VERSION

void
SystemInfo::GpuInit() {
    // initialize GPU information
    nvmlReturn_t nvml_result = nvmlInit();
    if (NVML_SUCCESS != nvml_result) {
        std::string msg = "System information initilization failed";
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::GpuUtilizationRatio(int64_t device_id) {
    try {
        GpuInit();
        nvmlMemory_t nvmlMemory;
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(device_id, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        return nvmlMemory.used / nvmlMemory.total;
    } catch (std::exception& ex) {
        std::string msg = "Cannot get gpu" + std::to_string(device_id) + " utilization ratio, reason: " + ex.what();
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::GpuTemperature(int64_t device_id) {
    try {
        GpuInit();
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(device_id, &device);
        unsigned int temperature;
        nvmlDeviceGetTemperature(device, NVML_TEMPERATURE_GPU, &temperature);
        return temperature;
    } catch (std::exception& ex) {
        std::string msg = "Cannot get gpu" + std::to_string(device_id) + " temperature, reason: " + ex.what();
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::GpuMemUsage(int64_t device_id) {
    try {
        GpuInit();
        nvmlMemory_t nvmlMemory;
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(device_id, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        return nvmlMemory.used;
    } catch (std::exception& ex) {
        std::string msg = "Cannot get gpu" + std::to_string(device_id) + " memory usage, reason: " + ex.what();
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::GpuMemTotal(int64_t device_id) {
    try {
        GpuInit();
        nvmlMemory_t nvmlMemory;
        nvmlDevice_t device;
        nvmlDeviceGetHandleByIndex(device_id, &device);
        nvmlDeviceGetMemoryInfo(device, &nvmlMemory);
        return nvmlMemory.total;
    } catch (std::exception& ex) {
        std::string msg = "Cannot get gpu" + std::to_string(device_id) + " memory usage, reason: " + ex.what();
        throw std::runtime_error(msg);
    }
}

int64_t
SystemInfo::GpuMemAvailable(int64_t device_id) {
    try {
        return GpuMemTotal(device_id) - GpuMemUsage(device_id);
    } catch (std::exception& ex) {
        std::string msg = "Cannot get gpu" + std::to_string(device_id) + " memory usage, reason: " + ex.what();
        throw std::runtime_error(msg);
    }
}

#endif

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

std::pair<int64_t, int64_t>
SystemInfo::Octets() {
    try {
        const std::string filename = "/proc/net/netstat";
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
        size_t space_pos = lastline.find(" ");
        while (space_pos != std::string::npos) {
            space_position.push_back(space_pos);
            space_pos = lastline.find(" ", space_pos + 1);
        }
        // InOctets is between 6th and 7th " " and OutOctets is between 7th and 8th " "
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
    } catch (std::exception& ex) {
        std::string msg = "Cannot get octets, reason: " + std::string(ex.what());
        throw std::runtime_error(msg);
    }
}

}  // namespace milvus
