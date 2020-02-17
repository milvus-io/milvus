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

#include <cstdio>

#include "Log.h"
#include "knowhere/common/Exception.h"

namespace knowhere {

KnowhereException::KnowhereException(const std::string& msg) : msg(msg) {
}

KnowhereException::KnowhereException(const std::string& m, const char* funcName, const char* file, int line) {
#ifdef DEBUG
    int size = snprintf(nullptr, 0, "Error in %s at %s:%d: %s", funcName, file, line, m.c_str());
    msg.resize(size + 1);
    snprintf(&msg[0], msg.size(), "Error in %s at %s:%d: %s", funcName, file, line, m.c_str());
#else
    std::string file_path(file);
    auto const pos = file_path.find_last_of('/');
    auto filename = file_path.substr(pos + 1).c_str();

    int size = snprintf(nullptr, 0, "Error in %s at %s:%d: %s", funcName, filename, line, m.c_str());
    msg.resize(size + 1);
    snprintf(&msg[0], msg.size(), "Error in %s at %s:%d: %s", funcName, filename, line, m.c_str());
#endif
}

const char*
KnowhereException::what() const noexcept {
    return msg.c_str();
}

}  // namespace knowhere
