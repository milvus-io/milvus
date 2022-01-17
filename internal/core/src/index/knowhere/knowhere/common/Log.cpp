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

#include "knowhere/common/Log.h"

#include <cstdarg>
#include <cstdio>
#include <memory>
#include <string>

namespace milvus {
namespace knowhere {

std::string
LogOut(const char* pattern, ...) {
    size_t len = strnlen(pattern, 1024) + 256;
    auto str_p = std::make_unique<char[]>(len);
    memset(str_p.get(), 0, len);

    va_list vl;
    va_start(vl, pattern);
    vsnprintf(str_p.get(), len - 1, pattern, vl);  // NOLINT
    va_end(vl);

    return std::string(str_p.get());
}

void
SetThreadName(const std::string& name) {
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

void
log_trace_(const std::string& s) {
    LOG_KNOWHERE_TRACE_ << s;
}

void
log_debug_(const std::string& s) {
    LOG_KNOWHERE_DEBUG_ << s;
}

void
log_info_(const std::string& s) {
    LOG_KNOWHERE_INFO_ << s;
}

void
log_warning_(const std::string& s) {
    LOG_KNOWHERE_WARNING_ << s;
}

void
log_error_(const std::string& s) {
    LOG_KNOWHERE_ERROR_ << s;
}

void
log_fatal_(const std::string& s) {
    LOG_KNOWHERE_FATAL_ << s;
}

}  // namespace knowhere
}  // namespace milvus
