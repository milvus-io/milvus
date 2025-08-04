// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "cachinglayer/Utils.h"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>

#include "log/Log.h"

#if defined(__linux__) || defined(__APPLE__)
#include <sys/statvfs.h>
#endif

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <mach/mach.h>
#endif

namespace milvus::cachinglayer::internal {

// Returns 0 if failed to get memory info, or if the platform is not supported.
int64_t
getHostTotalMemory() {
    static int64_t cached_total_memory = []() -> int64_t {
#ifdef __linux__
        std::ifstream meminfo("/proc/meminfo");
        if (!meminfo.is_open()) {
            LOG_WARN("[MCL] Failed to open /proc/meminfo");
            return 0;
        }

        std::string line;
        while (std::getline(meminfo, line)) {
            if (line.find("MemTotal:") == 0) {
                std::istringstream iss(line);
                std::string key;
                int64_t value;
                std::string unit;

                if (iss >> key >> value >> unit) {
                    // Convert kB to bytes
                    if (unit == "kB") {
                        value *= 1024;
                    }
                    return value;
                }
            }
        }
        return 0;
#elif defined(__APPLE__)
        int mib[2];
        mib[0] = CTL_HW;
        mib[1] = HW_MEMSIZE;
        int64_t size = 0;
        size_t len = sizeof(size);
        if (sysctl(mib, 2, &size, &len, NULL, 0) == 0) {
            return size;
        }
        return 0;
#else
        LOG_WARN(
            "[MCL] Host memory detection not implemented for this platform");
        return 0;
#endif
    }();
    return cached_total_memory;
}

// Impl based on pkg/util/hardware/container_linux.go::getContainerMemLimit()
// Returns 0 if failed to get memory limit, or if the platform is not supported.
int64_t
getContainerMemLimit() {
#ifdef __linux__
    std::vector<int64_t> limits;

    // Check MEM_LIMIT environment variable (Docker/container override)
    const char* mem_limit_env = std::getenv("MEM_LIMIT");
    if (mem_limit_env) {
        try {
            int64_t env_limit = std::stoll(mem_limit_env);
            limits.push_back(env_limit);
            LOG_TRACE("[MCL] Found MEM_LIMIT environment variable: {}",
                      FormatBytes(env_limit));
        } catch (...) {
            LOG_WARN("[MCL] Invalid MEM_LIMIT environment variable: {}",
                     mem_limit_env);
        }
    }

    // Check direct cgroup v1 path
    std::ifstream cgroup_v1("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    if (cgroup_v1.is_open()) {
        std::string limit_str;
        if (std::getline(cgroup_v1, limit_str)) {
            try {
                int64_t limit = std::stoll(limit_str);
                if (limit < (1LL << 62)) {  // Filter out unrealistic values
                    limits.push_back(limit);
                    LOG_TRACE(
                        "[MCL] Found direct cgroups v1 limit "
                        "(/sys/fs/cgroup/memory/memory.limit_in_bytes): {}",
                        FormatBytes(limit));
                }
            } catch (...) {
                // Ignore parse errors
            }
        }
        cgroup_v1.close();
    }

    // Check direct cgroup v2 path
    std::ifstream cgroup_v2("/sys/fs/cgroup/memory.max");
    if (cgroup_v2.is_open()) {
        std::string limit_str;
        if (std::getline(cgroup_v2, limit_str) && limit_str != "max") {
            try {
                int64_t limit = std::stoll(limit_str);
                limits.push_back(limit);
                LOG_TRACE(
                    "[MCL] Found direct cgroups v2 limit "
                    "(/sys/fs/cgroup/memory.max): {}",
                    FormatBytes(limit));
            } catch (...) {
                // Ignore parse errors
            }
        }
        cgroup_v2.close();
    }

    // Check process-specific cgroup limits from /proc/self/cgroup
    std::ifstream proc_cgroup("/proc/self/cgroup");
    if (proc_cgroup.is_open()) {
        std::string line;
        while (std::getline(proc_cgroup, line)) {
            // Look for memory controller lines
            if (line.find(":memory:") != std::string::npos ||
                line.find(":0:") != std::string::npos) {
                size_t last_colon = line.find_last_of(':');
                if (last_colon != std::string::npos) {
                    std::string cgroup_path = line.substr(last_colon + 1);

                    // Try v2 path
                    std::string v2_path =
                        "/sys/fs/cgroup" + cgroup_path + "/memory.max";
                    std::ifstream proc_v2(v2_path);
                    if (proc_v2.is_open()) {
                        std::string proc_line;
                        if (std::getline(proc_v2, proc_line) &&
                            proc_line != "max") {
                            try {
                                int64_t proc_limit = std::stoll(proc_line);
                                limits.push_back(proc_limit);
                                LOG_TRACE(
                                    "[MCL] Found process-specific cgroups v2 "
                                    "limit: {}",
                                    FormatBytes(proc_limit));
                            } catch (...) {
                                // Ignore parse errors
                            }
                        }
                    }

                    // Try v1 path
                    std::string v1_path = "/sys/fs/cgroup/memory" +
                                          cgroup_path +
                                          "/memory.limit_in_bytes";
                    std::ifstream proc_v1(v1_path);
                    if (proc_v1.is_open()) {
                        std::string proc_line;
                        if (std::getline(proc_v1, proc_line)) {
                            try {
                                int64_t proc_limit = std::stoll(proc_line);
                                // Filters out unrealistic cgroups v1 values (sometimes returns very large numbers
                                // when unlimited)
                                if (proc_limit < (1LL << 62)) {
                                    limits.push_back(proc_limit);
                                    LOG_TRACE(
                                        "[MCL] Found process-specific cgroups "
                                        "v1 limit: {}",
                                        FormatBytes(proc_limit));
                                }
                            } catch (...) {
                                // Ignore parse errors
                            }
                        }
                    }
                }
                break;  // Found memory controller, no need to continue
            }
        }
    }

    // Return the minimum of all found limits
    if (!limits.empty()) {
        int64_t min_limit = *std::min_element(limits.begin(), limits.end());
        LOG_TRACE("[MCL] Using minimum memory limit: {} from {} sources",
                  FormatBytes(min_limit),
                  limits.size());
        return min_limit;
    }

#else
    LOG_WARN(
        "[MCL] Container/cgroup memory limit detection not implemented for "
        "this platform");
#endif
    return 0;
}

SystemResourceInfo
getSystemMemoryInfo() {
    SystemResourceInfo info;

    // Get total memory (host vs container)
    int64_t host_memory = getHostTotalMemory();
    int64_t container_limit = getContainerMemLimit();

    if (host_memory == 0 && container_limit == 0) {
        // This indicates an error or unsupported platform, assume unlimited.
        info.total_bytes = std::numeric_limits<int64_t>::max();
        info.used_bytes = 0;
        return info;
    }

    if (container_limit > 0 && container_limit < host_memory) {
        info.total_bytes = container_limit;
        LOG_DEBUG("[MCL] Using container memory limit: {}",
                  FormatBytes(container_limit));
    } else {
        info.total_bytes = host_memory;
        if (container_limit > host_memory) {
            LOG_WARN(
                "[MCL] Container limit ({}) exceeds host memory ({}), using "
                "host memory",
                FormatBytes(container_limit),
                FormatBytes(host_memory));
        }
    }

    // Get current process memory usage (RSS - Shared)
    info.used_bytes = getCurrentProcessMemoryUsage();

    return info;
}

SystemResourceInfo
getSystemDiskInfo(const std::string& disk_path) {
    SystemResourceInfo info;
    // if we can't get disk info, return infinity and abandon disk protection.
    info.total_bytes = std::numeric_limits<int64_t>::max();
    info.used_bytes = 0;
    if (disk_path.empty()) {
        LOG_WARN("[MCL] Disk path is empty, returning 0");
        return info;
    }

#if defined(__linux__) || defined(__APPLE__)
    struct statvfs stat;
    if (statvfs(disk_path.c_str(), &stat) != 0) {
        LOG_WARN("[MCL] Failed to statvfs({}): {}", disk_path, strerror(errno));
        return info;
    }
    // Total bytes = f_blocks * f_frsize
    info.total_bytes = static_cast<int64_t>(stat.f_blocks) *
                       static_cast<int64_t>(stat.f_frsize);
    // Used bytes = (f_blocks - f_bfree) * f_frsize
    info.used_bytes = (static_cast<int64_t>(stat.f_blocks) -
                       static_cast<int64_t>(stat.f_bfree)) *
                      static_cast<int64_t>(stat.f_frsize);
#else
    LOG_WARN(
        "[MCL] Disk info not implemented for this platform, returning "
        "unlimited capacity");
#endif
    return info;
}

int64_t
getCurrentProcessMemoryUsage() {
#ifdef __linux__
    std::ifstream status("/proc/self/status");
    if (!status.is_open()) {
        LOG_WARN("[MCL] Failed to open /proc/self/status, returning 0");
        return 0;
    }

    int64_t rss = 0;
    int64_t shared = 0;
    std::string line;

    while (std::getline(status, line)) {
        if (line.find("VmRSS:") == 0) {
            std::istringstream iss(line);
            std::string key;
            int64_t value;
            std::string unit;

            if (iss >> key >> value >> unit) {
                // Convert kB to bytes
                if (unit == "kB") {
                    value *= 1024;
                }
                rss = value;
            }
        } else if (line.find("RssFile:") == 0) {
            std::istringstream iss(line);
            std::string key;
            int64_t value;
            std::string unit;

            if (iss >> key >> value >> unit) {
                // Convert kB to bytes
                if (unit == "kB") {
                    value *= 1024;
                }
                shared = value;
            }
        }
    }

    // Return RSS - Shared (file-backed memory) to match Go implementation
    return rss - shared;
#elif defined(__APPLE__)
    task_vm_info_data_t vm_info;
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(
            mach_task_self(), TASK_VM_INFO, (task_info_t)&vm_info, &count) !=
        KERN_SUCCESS) {
        LOG_WARN("[MCL] Failed to get task info for current process");
        return 0;
    }
    return vm_info.internal;
#else
    LOG_WARN(
        "[MCL] Process memory monitoring not implemented for this platform");
    return 0;
#endif
}

}  // namespace milvus::cachinglayer::internal
