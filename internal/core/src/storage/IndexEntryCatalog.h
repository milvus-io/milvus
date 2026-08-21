// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"

namespace milvus::storage {

struct PlainEntrySource {
    // Absolute offset in the packed V3 object.
    uint64_t remote_offset;
    size_t remote_bytes;
};

struct EncryptedSliceSource {
    // Absolute offset in the packed V3 object.
    uint64_t remote_offset;
    size_t remote_bytes;
    size_t target_offset;
    size_t target_bytes;
};

struct EncryptedEntrySource {
    size_t plaintext_size;
    std::vector<EncryptedSliceSource> slices;
};

struct IndexEntryCatalogEntry {
    std::string name;
    size_t plaintext_size;
    uint32_t expected_crc;
    std::variant<PlainEntrySource, EncryptedEntrySource> source;
};

class IndexEntryCatalog {
 public:
    const std::vector<IndexEntryCatalogEntry>&
    Entries() const noexcept {
        return entries_;
    }

    const IndexEntryCatalogEntry&
    At(std::string_view name) const;

    bool
    HasEntry(std::string_view name) const noexcept {
        return std::any_of(
            entries_.begin(), entries_.end(), [name](const auto& entry) {
                return entry.name == name;
            });
    }

    template <typename T>
    T
    GetMeta(const std::string& key) const {
        AssertInfo(metadata_.contains(key), "Meta key not found: {}", key);
        return metadata_[key].get<T>();
    }

    template <typename T>
    T
    GetMeta(const std::string& key, const T& default_value) const {
        if (!metadata_.contains(key)) {
            return default_value;
        }
        return metadata_[key].get<T>();
    }

    bool
    HasMeta(const std::string& key) const {
        return metadata_.contains(key);
    }

 private:
    friend class IndexEntryReader;

    std::vector<IndexEntryCatalogEntry> entries_;
    nlohmann::json metadata_;
};

}  // namespace milvus::storage
