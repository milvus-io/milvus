// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "log/Log.h"
INITIALIZE_EASYLOGGINGPP

#ifdef WIN32
#include <Windows.h>
#endif
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>

// namespace milvus {

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
#ifdef __APPLE__
    pthread_setname_np(name.c_str());
#elif __linux__
    pthread_setname_np(pthread_self(), name.c_str());
#else
#error "Unsupported SetThreadName";
#endif
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

#ifndef WIN32

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
#ifdef __APPLE__
    uint64_t tid;
    pthread_threadid_np(NULL, &tid);
#elif __linux__
    int64_t tid = gettid();
#else
#error "Unsupported SetThreadName";
#endif

    int64_t pid = getpid();
    char filename[256];
    snprintf(filename, sizeof(filename), "/proc/%lld/task/%lld/stat", (long long)pid, (long long)tid);  // NOLINT

    int64_t val = 0;
    char comm[16], state;
    FILE* thread_stat = fopen(filename, "r");
    auto ret = fscanf(thread_stat, "%lld %s %s ", (long long*)&val, comm, &state);  // NOLINT

    for (auto i = 4; i < 23; i++) {
        ret = fscanf(thread_stat, "%lld ", (long long*)&val);  // NOLINT
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

#else

#define WINDOWS_TICK 10000000
#define SEC_TO_UNIX_EPOCH 11644473600LL

int64_t
get_thread_start_timestamp() {
    FILETIME dummy;
    FILETIME ret;

    if (GetThreadTimes(GetCurrentThread(), &ret, &dummy, &dummy, &dummy)) {
        auto ticks = Int64ShllMod32(ret.dwHighDateTime, 32) | ret.dwLowDateTime;
        auto thread_started = ticks / WINDOWS_TICK - SEC_TO_UNIX_EPOCH;
        return get_now_timestamp() - thread_started;
    }
    return 0;
}

#endif

// }  // namespace milvus
