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

#pragma once

#include <exception>
#include <string>

namespace milvus {
namespace knowhere {

class KnowhereException : public std::exception {
 public:
    explicit KnowhereException(std::string msg);

    KnowhereException(const std::string& msg, const char* funName, const char* file, int line);

    const char*
    what() const noexcept override;

    std::string msg_;
};

#define KNOHWERE_ERROR_MSG(MSG) printf("%s", KnowhereException(MSG, __PRETTY_FUNCTION__, __FILE__, __LINE__).what())

#define KNOWHERE_THROW_MSG(MSG)                                                                  \
    do {                                                                                         \
        throw milvus::knowhere::KnowhereException(MSG, __PRETTY_FUNCTION__, __FILE__, __LINE__); \
    } while (false)

#define KNOHERE_THROW_FORMAT(FMT, ...)                                             \
    do {                                                                           \
        std::string __s;                                                           \
        int __size = snprintf(nullptr, 0, FMT, __VA_ARGS__);                       \
        __s.resize(__size + 1);                                                    \
        snprintf(&__s[0], __s.size(), FMT, __VA_ARGS__);                           \
        throw faiss::FaissException(__s, __PRETTY_FUNCTION__, __FILE__, __LINE__); \
    } while (false)

}  // namespace knowhere
}  // namespace milvus
