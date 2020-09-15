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

#include <yaml-cpp/yaml.h>
#include <fstream>
#include <iostream>
#include <regex>
#include <unordered_map>

#include "config/ConfigMgr.h"
#include "utils/Json.h"

namespace {
void
Flatten(const YAML::Node& node, std::unordered_map<std::string, std::string>& target, const std::string& prefix) {
    for (auto& it : node) {
        auto key = prefix.empty() ? it.first.as<std::string>() : prefix + "." + it.first.as<std::string>();
        switch (it.second.Type()) {
            case YAML::NodeType::Null: {
                target[key] = "";
                break;
            }
            case YAML::NodeType::Scalar: {
                target[key] = it.second.as<std::string>();
                break;
            }
            case YAML::NodeType::Sequence: {
                std::string value;
                for (auto& sub : it.second) value += sub.as<std::string>() + ",";
                target[key] = value;
                break;
            }
            case YAML::NodeType::Map: {
                Flatten(it.second, target, key);
                break;
            }
            case YAML::NodeType::Undefined: {
                throw "Unexpected";
            }
            default:
                break;
        }
    }
}

void
ThrowIfNotSuccess(const milvus::ConfigStatus& cs) {
    if (cs.set_return != milvus::SetReturn::SUCCESS) {
        throw cs;
    }
}

};  // namespace

namespace milvus {

extern std::mutex&
GetConfigMutex();

extern std::unordered_map<std::string, BaseConfigPtr>
InitConfig();

extern const char* config_file_template;

void
BaseConfigMgr::Attach(const std::string& name, ConfigObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    observers_[name].push_back(observer);
}

void
BaseConfigMgr::Detach(const std::string& name, ConfigObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end()) {
        return;
    }
    auto& ob_list = observers_[name];
    ob_list.remove(observer);
}

void
BaseConfigMgr::Notify(const std::string& name) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end()) {
        return;
    }
    auto& ob_list = observers_[name];
    for (auto& ob : ob_list) {
        ob->ConfigUpdate(name);
    }
}

ConfigMgr ConfigMgr::instance;

ConfigMgr::ConfigMgr() : config_list_(InitConfig()) {
    effective_immediately_ = {
        "cache.cache_size",
        "gpu.cache_size",
        "gpu.gpu_search_threshold",
        "storage.auto_flush_interval",
        "engine.build_index_threshold",
        "engine.search_combine_nq",
        "engine.use_blas_threshold",
        "engine.omp_thread_num",
    };
}

void
ConfigMgr::Init() {
    std::lock_guard<std::mutex> lock(GetConfigMutex());
    for (auto& kv : config_list_) {
        kv.second->Init();
    }
}

void
ConfigMgr::LoadFile(const std::string& path) {
    /* load from milvus.yaml */
    auto yaml = YAML::LoadFile(path);

    /* make it flattened */
    std::unordered_map<std::string, std::string> flattened;
    Flatten(yaml, flattened, "");

    /* update config */
    for (auto& it : flattened) Set(it.first, it.second, false);
}

void
ConfigMgr::LoadMemory(const std::string& yaml_string) {
    auto yaml = YAML::Load(yaml_string);

    /* make it flattened */
    std::unordered_map<std::string, std::string> flattened;
    Flatten(yaml, flattened, "");

    /* update config */
    for (auto& it : flattened) Set(it.first, it.second, false);
}

void
ConfigMgr::Set(const std::string& name, const std::string& value, bool update) {
    try {
        auto& config = config_list_.at(name);
        std::unique_lock<std::mutex> lock(GetConfigMutex());
        if (not update) {
            /* update=false when loading from config file */
            ThrowIfNotSuccess(config->Set(value, update));
        } else if (config->modifiable_) {
            /* set manually */

            /* If save failure, rollback. */
            auto old_value = config->Get();
            ThrowIfNotSuccess(config->Set(value, update));
            try {
                Save(FilePath());
                lock.unlock();
                Notify(name);
                /* if not found in effective immediately list, need restart */
                if (effective_immediately_.find(name) == effective_immediately_.end()) {
                    require_restart_ |= true;
                }
            } catch (...) {
                /* rollback */
                ThrowIfNotSuccess(config->Set(old_value, false));
                lock.unlock();
                throw;
            }
        } else {
            /* try to set a read-only value */
            throw ConfigStatus(SetReturn::IMMUTABLE, "Config " + name + " is immutable");
        }
    } catch (ConfigStatus& cs) {
        throw std::runtime_error(cs.message);
    } catch (std::out_of_range& ex) {
        throw std::runtime_error("Config " + name + " not found.");
    } catch (std::exception& ex) {
        throw;
    } catch (...) {
        throw std::runtime_error("Unexpected exception happened when setting config " + name + ".");
    }
}

std::string
ConfigMgr::Get(const std::string& name) const {
    try {
        auto& config = config_list_.at(name);
        std::lock_guard<std::mutex> lock(GetConfigMutex());
        return config->Get();
    } catch (std::out_of_range& ex) {
        throw std::runtime_error("Config " + name + " not found.");
    } catch (...) {
        throw std::runtime_error("Unexpected exception happened when getting config " + name + ".");
    }
}

std::string
ConfigMgr::Dump() const {
    std::stringstream ss;
    for (auto& kv : config_list_) {
        auto& config = kv.second;
        ss << config->name_ << ": " << config->Get() << std::endl;
    }
    return ss.str();
}

std::string
ConfigMgr::JsonDump() const {
    json config_list;
    for (auto& kv : config_list_) {
        auto& config = kv.second;
        config_list[config->name_] = config->Get();
    }
    return config_list.dump();
}

void
ConfigMgr::Save(const std::string& path) {
    if (path.empty()) {
        throw std::runtime_error("Cannot save config into empty path.");
    }

    std::string file_content(config_file_template);
    for (auto& config_pair : config_list_) {
        auto placeholder = "@" + config_pair.first + "@";
        file_content = std::regex_replace(file_content, std::regex(placeholder), config_pair.second->Get());
    }
    std::ofstream config_file(path);
    config_file << file_content;
    config_file.close();

    if (config_file.fail()) {
        throw std::runtime_error("Cannot save config into file: " + path + ".");
    }
}

}  // namespace milvus
