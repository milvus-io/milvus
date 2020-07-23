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
#include <string>
#include <unordered_map>
#include <vector>

#include "config/ServerConfig.h"

namespace milvus {

class ConfigObserver {
 public:
    virtual ~ConfigObserver() {
    }

    virtual void
    ConfigUpdate(const std::string& name) = 0;
};
using ConfigObserverPtr = std::shared_ptr<ConfigObserver>;

class ConfigMgr {
 public:
    static ConfigMgr&
    GetInstance() {
        return instance;
    }

 private:
    static ConfigMgr instance;

 public:
    ConfigMgr();

    ConfigMgr(const ConfigMgr&) = delete;
    ConfigMgr&
    operator=(const ConfigMgr&) = delete;

    ConfigMgr(ConfigMgr&&) = delete;
    ConfigMgr&
    operator=(ConfigMgr&&) = delete;

 public:
    void
    Init();

    void
    Load(const std::string& path);

    void
    Set(const std::string& name, const std::string& value, bool update = true);

    std::string
    Get(const std::string& name) const;

    std::string
    Dump() const;

 public:
    // Shared pointer should not be used here
    void
    Attach(const std::string& name, ConfigObserver* observer);

    void
    Detach(const std::string& name, ConfigObserver* observer);

 private:
    void
    Notify(const std::string& name);

 private:
    std::vector<BaseConfigPtr> config_list_;
    std::mutex mutex_;

    std::unordered_map<std::string, std::list<ConfigObserver*>> observers_;
    std::mutex observer_mutex_;
};

}  // namespace milvus
