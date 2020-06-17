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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cache/DataObj.h"

namespace milvus {
namespace Attr {

class Attr : public milvus::cache::DataObj {
 public:
    Attr() = default;

    Attr(std::unordered_map<std::string, std::vector<uint8_t>> attr_data,
         std::unordered_map<std::string, int64_t> attr_size, int64_t entity_count)
        : attr_data_(std::move(attr_data)), attr_size_(std::move(attr_size)), entity_count_(entity_count) {
    }

    void
    SetAttrData(std::unordered_map<std::string, std::vector<uint8_t>> attr_data) {
        attr_data_ = std::move(attr_data);
    }

    void
    SetAttrSize(std::unordered_map<std::string, int64_t> attr_size) {
        attr_size_ = std::move(attr_size);
    }

    void
    SetEntityCount(int64_t entity_count) {
        entity_count_ = entity_count;
    }

    std::unordered_map<std::string, std::vector<uint8_t>>
    attr_data() {
        return attr_data_;
    }

    std::unordered_map<std::string, int64_t>
    attr_size() {
        return attr_size_;
    }

    int64_t
    entity_count() {
        return entity_count_;
    }

    int64_t
    attr_data_size() {
        int64_t attr_data_size = 0;
        auto attr_it = attr_size_.begin();
        for (; attr_it != attr_size_.end(); attr_it++) {
            attr_data_size += attr_it->first.size() + attr_it->second;
        }
        return attr_data_size;
    }

    int64_t
    Size() override {
        return attr_data_size();
    }

 private:
    std::unordered_map<std::string, std::vector<uint8_t>> attr_data_;
    std::unordered_map<std::string, int64_t> attr_size_;
    int64_t entity_count_;
};

using AttrPtr = std::shared_ptr<Attr>;

}  // namespace Attr
}  // namespace milvus
