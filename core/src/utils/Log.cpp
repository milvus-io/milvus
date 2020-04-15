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
#include "utils/Log.h"

#include <cstdarg>
#include <cstdio>
#include <memory>
#include <string>

namespace milvus {

std::string
LogOut(const char* pattern, ...) {
    size_t len = strnlen(pattern, 1024) + 256;
    auto str_p = std::make_unique<char[]>(len);
    memset(str_p.get(), 0, len);

    va_list vl;
    va_start(vl, pattern);
    vsnprintf(str_p.get(), len, pattern, vl);
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

}  // namespace milvus
