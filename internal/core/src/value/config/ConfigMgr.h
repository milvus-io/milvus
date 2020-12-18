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

#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "value/ValueMgr.h"
#include "value/ValueType.h"

namespace milvus {

class ConfigObserver : public ValueObserver {
 public:
    void
    ValueUpdate(const std::string& name) override {
        ConfigUpdate(name);
    }

    virtual void
    ConfigUpdate(const std::string& name) = 0;
};

class ConfigMgr : public ValueMgr {
 public:
    static ConfigMgr&
    GetInstance() {
        return instance;
    }

 private:
    static ConfigMgr instance;

 public:
    bool
    RequireRestart() {
        return require_restart_;
    }

 public:
    ConfigMgr();

    /* throws std::exception only */
    void
    LoadFile(const std::string& path);

    /* for testing */
    /* throws std::exception only */
    void
    LoadMemory(const std::string& yaml_string);

    /* throws std::exception only */
    void
    Set(const std::string& name, const std::string& value, bool update) override;

    /* throws std::exception only */
    std::string
    Get(const std::string& name) const override;

 private:
    struct SaveValueError : public std::exception {
        explicit SaveValueError(const std::string& msg) : message(msg) {
        }
        const std::string message;
    };

    void
    Save();

 private:
    const std::unordered_map<std::string, BaseValuePtr>& config_list_ = value_list_;
    std::string config_file_;
    bool require_restart_ = false;
    std::set<std::string> effective_immediately_;
};

}  // namespace milvus
