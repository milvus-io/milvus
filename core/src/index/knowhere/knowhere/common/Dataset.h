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

#include <any>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace milvus {
namespace knowhere {

using Value = std::any;
using ValuePtr = std::shared_ptr<Value>;

class Dataset {
 public:
    Dataset() = default;

    template <typename T>
    void
    Set(const std::string& k, T&& v) {
        std::lock_guard<std::mutex> lk(mutex_);
        data_[k] = std::make_shared<Value>(std::forward<T>(v));
    }

    template <typename T>
    T
    Get(const std::string& k) {
        std::lock_guard<std::mutex> lk(mutex_);
        try {
            return std::any_cast<T>(*(data_.at(k)));
        } catch (...) {
            throw std::logic_error("Can't find this key");
        }
    }

    const std::map<std::string, ValuePtr>&
    data() const {
        return data_;
    }

 private:
    std::mutex mutex_;
    std::map<std::string, ValuePtr> data_;
};
using DatasetPtr = std::shared_ptr<Dataset>;

}  // namespace knowhere
}  // namespace milvus
