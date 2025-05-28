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

#include "common/EasyAssert.h"
#include "fmt/core.h"
#include "log/Log.h"

/*
 * INITIALIZE_EASYLOGGINGPP will create a global variable whose name is same to that already created in knowhere,
 * which will lead a `double-free` bug when the program exits.
 * For why this issue happened please refer to
 * https://gcc-help.gcc.gnu.narkive.com/KZGaXRNr/global-variable-in-static-library-double-free-or-corruption-error.
 */
// INITIALIZE_EASYLOGGINGPP

#ifdef WIN32
#include <Windows.h>
#endif
#include <chrono>
#include <cstdarg>
#include <iostream>
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
    int result = vsnprintf(str_p.get(), len, pattern, vl);  // NOLINT
    va_end(vl);

    if (result < 0) {
        std::cerr << "Error: vsnprintf failed to format the string."
                  << std::endl;
        return "Formatting Error";
    } else if (static_cast<size_t>(result) >= len) {
        std::cerr
            << "Warning: Output was truncated. Buffer size was insufficient."
            << std::endl;
        return "Truncated Output";
    }

    return {str_p.get()};
}

void
SetThreadName(const std::string_view name) {
    // Note: the name cannot exceed 16 bytes
#ifdef __APPLE__
    pthread_setname_np(name.data());
#elif defined(__linux__) || defined(__MINGW64__)
    pthread_setname_np(pthread_self(), name.data());
#else
#error "Unsupported SetThreadName";
#endif
}

std::string
GetThreadName() {
    std::string thread_name = "unnamed";
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
get_thread_starttime() {
#ifdef __APPLE__
    uint64_t tid;
    pthread_threadid_np(nullptr, &tid);
#elif __linux__
    int64_t tid = gettid();
#else
#error "Unsupported SetThreadName";
#endif

    int64_t pid = getpid();
    char filename[256];
    int ret_snprintf =
        snprintf(filename,
                 sizeof(filename),
                 "/proc/%lld/task/%lld/stat",
                 (long long)pid,   // NOLINT, TODO: How to solve this?
                 (long long)tid);  // NOLINT

    if (ret_snprintf < 0 ||
        static_cast<size_t>(ret_snprintf) >= sizeof(filename)) {
        std::cerr << "Error: snprintf failed or output was truncated when "
                     "creating filename."
                  << std::endl;
        throw std::runtime_error("Failed to format filename string.");
    }

    int64_t val = 0;
    char comm[16], state;
    FILE* thread_stat = fopen(filename, "r");
    AssertInfo(thread_stat != nullptr, "opening file:{} failed!", filename);
    auto ret = fscanf(
        thread_stat, "%lld %s %s ", (long long*)&val, comm, &state);  // NOLINT

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

#else

#define WINDOWS_TICK 10000000
#define SEC_TO_UNIX_EPOCH 11644473600LL

#endif

// }  // namespace milvus
