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

#include "value/ValueMgr.h"
#include "utils/Json.h"

namespace milvus {

void
BaseValueMgr::Attach(const std::string& name, ValueObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    observers_[name].push_back(observer);
}

void
BaseValueMgr::Detach(const std::string& name, ValueObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end()) {
        return;
    }
    auto& ob_list = observers_[name];
    ob_list.remove(observer);
}

void
BaseValueMgr::Notify(const std::string& name) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end()) {
        return;
    }
    auto& ob_list = observers_[name];
    for (auto& ob : ob_list) {
        ob->ValueUpdate(name);
    }
}

void
ValueMgr::Init() {
    for (auto& kv : value_list_) {
        kv.second->Init();
    }
}

std::string
ValueMgr::Dump() const {
    std::stringstream ss;
    for (auto& kv : value_list_) {
        auto& config = kv.second;
        ss << config->name_ << ": " << config->Get() << std::endl;
    }
    return ss.str();
}

std::string
ValueMgr::JsonDump() const {
    json config_list;
    for (auto& kv : value_list_) {
        auto& config = kv.second;
        config_list[config->name_] = config->Get();
    }
    return config_list.dump();
}

}  // namespace milvus
