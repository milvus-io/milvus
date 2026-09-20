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

#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::storage {

// Own the mutable mkdtemp template before creating the directory. Successful
// creation arms cleanup without an allocation; the configured parent is never
// owned. The containing object must already have its final owner when Create
// is called.
class LocalDirectory {
 public:
    explicit LocalDirectory(std::string pattern) : path_(std::move(pattern)) {
    }

    LocalDirectory(const LocalDirectory&) = delete;
    LocalDirectory&
    operator=(const LocalDirectory&) = delete;

    static std::shared_ptr<LocalDirectory>
    CreateOwned(const std::string& parent,
                const char* pattern,
                std::string_view context) {
        auto result = std::make_shared<LocalDirectory>(
            Pattern(parent, pattern, context));
        result->Create(parent, context);
        return result;
    }

    ~LocalDirectory() {
        if (created_) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    static std::string
    Pattern(const std::string& parent,
            const char* pattern,
            std::string_view context) {
        if (parent.empty()) {
            ThrowInfo(UnexpectedError, "{} staging parent is empty", context);
        }
        std::error_code error;
        std::filesystem::create_directories(parent, error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create {} staging root {}: {}",
                      context,
                      parent,
                      error.message());
        }
        return (std::filesystem::path(parent) / pattern).string();
    }

    void
    Create(const std::string& parent, std::string_view context) {
        if (::mkdtemp(path_.data()) == nullptr) {
            ThrowInfo(FileCreateFailed,
                      "failed to create {} staging directory in {}: {}",
                      context,
                      parent,
                      std::strerror(errno));
        }
        created_ = true;
    }

    const std::string&
    Path() const {
        return path_;
    }

    size_t
    PathHeapBytes() const {
        const auto inline_capacity = std::string{}.capacity();
        if (path_.capacity() <= inline_capacity) {
            return 0;
        }
        if (path_.capacity() == std::numeric_limits<size_t>::max()) {
            ThrowInfo(DataFormatBroken,
                      "local directory path memory size overflows");
        }
        return path_.capacity() + 1;
    }

    size_t
    HeapBytes() const {
        const auto path_bytes = PathHeapBytes();
        if (path_bytes >
            std::numeric_limits<size_t>::max() - sizeof(LocalDirectory)) {
            ThrowInfo(DataFormatBroken,
                      "local directory memory size overflows");
        }
        return sizeof(LocalDirectory) + path_bytes;
    }

    bool
    Owns(const std::string& path) const {
        if (!created_ || path.empty()) {
            return false;
        }
        std::error_code error;
        const auto base = std::filesystem::weakly_canonical(path_, error);
        if (error) {
            return false;
        }
        const auto candidate = std::filesystem::weakly_canonical(path, error);
        if (error || candidate == base) {
            return false;
        }
        auto base_it = base.begin();
        auto candidate_it = candidate.begin();
        for (; base_it != base.end(); ++base_it, ++candidate_it) {
            if (candidate_it == candidate.end() || *base_it != *candidate_it) {
                return false;
            }
        }
        return true;
    }

 private:
    std::string path_;
    bool created_{false};
};

}  // namespace milvus::storage
