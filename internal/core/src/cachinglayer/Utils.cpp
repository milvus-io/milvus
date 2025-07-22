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

#include "log/Log.h"

namespace milvus::cachinglayer::internal {

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
#else
        LOG_WARN(
            "[MCL] Host memory detection not implemented for this platform");
        return 0;
#endif
    }();
    return cached_total_memory;
}

// Impl based on pkg/util/hardware/container_linux.go::getContainerMemLimit()
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
            LOG_DEBUG("[MCL] Found MEM_LIMIT environment variable: {} bytes",
                      env_limit);
        } catch (...) {
            LOG_WARN("[MCL] Invalid MEM_LIMIT environment variable: {}",
                     mem_limit_env);
        }
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
                                LOG_DEBUG(
                                    "[MCL] Found process-specific cgroups v2 "
                                    "limit: {} bytes",
                                    proc_limit);
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
                                    LOG_DEBUG(
                                        "[MCL] Found process-specific cgroups "
                                        "v1 limit: {} bytes",
                                        proc_limit);
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
        LOG_DEBUG("[MCL] Using minimum memory limit: {} bytes from {} sources",
                  min_limit,
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

SystemMemoryInfo
getSystemMemoryInfo() {
    SystemMemoryInfo info;

    // Get total memory (host vs container)
    int64_t host_memory = getHostTotalMemory();
    int64_t container_limit = getContainerMemLimit();

    if (container_limit > 0 && container_limit < host_memory) {
        info.total_memory_bytes = container_limit;
        LOG_DEBUG("[MCL] Using container memory limit: {} bytes",
                  container_limit);
    } else {
        info.total_memory_bytes = host_memory;
        if (container_limit > host_memory) {
            LOG_WARN(
                "[MCL] Container limit ({} bytes) exceeds host memory ({} "
                "bytes), using host memory",
                container_limit,
                host_memory);
        }
    }

    // Get current process memory usage (RSS - Shared)
    info.used_memory_bytes = getCurrentProcessMemoryUsage();
    info.available_memory_bytes =
        info.total_memory_bytes - info.used_memory_bytes;

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
#else
    LOG_WARN(
        "[MCL] Process memory monitoring not implemented for this platform");
    return 0;
#endif
}

}  // namespace milvus::cachinglayer::internal