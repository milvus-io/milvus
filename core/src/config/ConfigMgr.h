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

#include "config/ConfigType.h"

namespace milvus {

class ConfigObserver {
 public:
    virtual ~ConfigObserver() = default;

    virtual void
    ConfigUpdate(const std::string& name) = 0;
};
using ConfigObserverPtr = std::shared_ptr<ConfigObserver>;

class BaseConfigMgr {
 protected:
    BaseConfigMgr() = default;

 public:
    // Shared pointer should not be used here
    void
    Attach(const std::string& name, ConfigObserver* observer);

    void
    Detach(const std::string& name, ConfigObserver* observer);

 protected:
    void
    Notify(const std::string& name);

 private:
    std::unordered_map<std::string, std::list<ConfigObserver*>> observers_;
    std::mutex observer_mutex_;
};

class ConfigMgr : public BaseConfigMgr {
 public:
    static ConfigMgr&
    GetInstance() {
        return instance;
    }

 private:
    static ConfigMgr instance;

    /* TODO: move into ServerConfig */
 public:
    std::string&
    FilePath() {
        return config_file_;
    }

    bool
    RequireRestart() {
        return require_restart_;
    }

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
    LoadFile(const std::string& path);

    /* for testing */
    void
    LoadMemory(const std::string& yaml_string);

    /* throws std::exception only */
    void
    Set(const std::string& name, const std::string& value, bool update = true);

    /* throws std::exception only */
    std::string
    Get(const std::string& name) const;

    std::string
    Dump() const;

    std::string
    JsonDump() const;

 private:
    void
    Save(const std::string& path);

 private:
    const std::unordered_map<std::string, BaseConfigPtr> config_list_;
    std::mutex mutex_;
    std::string config_file_;
    bool require_restart_ = false;
    std::set<std::string> effective_immediately_;
};

}  // namespace milvus
