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
#include <string>
#include <map>
#include <vector>
#include "knowhere/index_sequence.h"
namespace milvus {

typedef knowhere::IndexSequence Binary;
typedef uint8_t* BianryValuePtr;
class BinarySet {
 public:
    BinarySet() = default;

    const size_t
    GetBinarySetSize() {
        return binary_map_.size();
    }

    const std::vector<std::string>
    GetBinarySetAllKeys() const {
        std::vector<std::string> keys;
        keys.reserve(binary_map_.size());
        for (auto& kv : binary_map_) {
            keys.push_back(kv.first);
        }
        return keys;
    }

    const BianryValuePtr
    GetBinaryPtrByName(const std::string& name) const {
        if (Contains(name)) {
            auto& index_seq = binary_map_.at(name);
            return index_seq.GetSeq();
        }
        return nullptr;
    }

    size_t
    GetBinarySizeByName(const std::string& name) const {
        if (Contains(name)) {
            auto& index_seq = binary_map_.at(name);
            return index_seq.GetSize();
        }
        return 0;
    }

    void
    Append(const std::string& name, Binary&& binary) {
        binary_map_[name] = std::move(binary);
    }

    Binary
    Erase(const std::string& name) {
        auto it = binary_map_.find(name);
        if (it != binary_map_.end()) {
            auto result = std::move(it->second);
            binary_map_.erase(it);
            return std::move(result);
        } else {
            return Binary(nullptr, 0);
        }
    }

    void
    clear() {
        binary_map_.clear();
    }

    bool
    Contains(const std::string& key) const {
        return binary_map_.find(key) != binary_map_.end();
    }

 private:
    std::map<std::string, Binary> binary_map_;
};
}  // namespace milvus