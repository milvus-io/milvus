// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstdio>
#include <cstring>
#include <utility>

#include "Log.h"
#include "knowhere/common/Exception.h"

namespace milvus {
namespace knowhere {

KnowhereException::KnowhereException(std::string msg) : msg_(std::move(msg)) {
}

KnowhereException::KnowhereException(const std::string& m, const char* funcName, const char* file, int line) {
    const char* filename = funcName;
    while (auto tmp = strchr(filename, '/')) {
        filename = tmp + 1;
    }
    int size = snprintf(nullptr, 0, "Error in %s at %s:%d: %s", funcName, filename, line, m.c_str());
    msg_.resize(size + 1);
    snprintf(msg_.data(), m.size(), "Error in %s at %s:%d: %s", funcName, filename, line, m.c_str());
}

const char*
KnowhereException::what() const noexcept {
    return msg_.c_str();
}

}  // namespace knowhere
}  // namespace milvus
