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

#include "log/Log.h"
INITIALIZE_EASYLOGGINGPP

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>

namespace milvus {

std::string
LogOut(const char* pattern, ...) {
    size_t len = strnlen(pattern, 1024) + 256;
    auto str_p = std::make_unique<char[]>(len);
    memset(str_p.get(), 0, len);

    va_list vl;
    va_start(vl, pattern);
    vsnprintf(str_p.get(), len, pattern, vl);  // NOLINT
    va_end(vl);

    return std::string(str_p.get());
}

void
SetThreadName(const std::string& name) {
    // Note: the name cannot exceed 16 bytes
    pthread_setname_np(pthread_self(), name.c_str());
}

std::string
GetThreadName() {
    std::string thread_name = "unamed";
    char name[16];
    size_t len = 16;
    auto err = pthread_getname_np(pthread_self(), name, len);
    if (not err) {
        thread_name = name;
    }

    return thread_name;
}

int64_t
get_now_timestamp() {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::seconds>(now).count();
}

int64_t
get_system_boottime() {
    FILE* uptime = fopen("/proc/uptime", "r");
    float since_sys_boot, _;
    auto ret = fscanf(uptime, "%f %f", &since_sys_boot, &_);
    fclose(uptime);
    if (ret != 2) {
        throw std::runtime_error("read /proc/uptime failed.");
    }
    return static_cast<int64_t>(since_sys_boot);
}

int64_t
get_thread_starttime() {
    int64_t tid = gettid();
    int64_t pid = getpid();
    char filename[256];
    snprintf(filename, sizeof(filename), "/proc/%ld/task/%ld/stat", pid, tid);

    int64_t val = 0;
    char comm[16], state;
    FILE* thread_stat = fopen(filename, "r");
    auto ret = fscanf(thread_stat, "%ld %s %s ", &val, comm, &state);
    for (auto i = 4; i < 23; i++) {
        ret = fscanf(thread_stat, "%ld ", &val);
        if (i == 22) {
            break;
        }
    }
    fclose(thread_stat);
    if (ret != 1) {
        throw std::runtime_error("read " + std::string(filename) + " failed.");
    }
    return val / sysconf(_SC_CLK_TCK);
}

int64_t
get_thread_start_timestamp() {
    try {
        return get_now_timestamp() - get_system_boottime() + get_thread_starttime();
    } catch (...) {
        return 0;
    }
}

}  // namespace milvus
