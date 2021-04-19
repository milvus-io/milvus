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
#include <cstring>
#include <limits>
#include <unordered_map>
#include <iostream>
#include "config/ConfigMgr.h"
#include "config/ServerConfig.h"

namespace {
const int64_t MB = (1024ll * 1024);
const int64_t GB = (1024ll * 1024 * 1024);

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

ConfigMgr ConfigMgr::instance;

ConfigMgr::ConfigMgr() {
    config_list_ = {

        /* general */
        {"timezone", CreateStringConfig("timezone", false, &config.timezone.value, "UTC+8", nullptr, nullptr)},

        /* network */
        {"network.address",
         CreateStringConfig("network.address", false, &config.network.address.value, "0.0.0.0", nullptr, nullptr)},
        {"network.port",
         CreateIntegerConfig("network.port", false, 0, 65535, &config.network.port.value, 19530, nullptr, nullptr)},

        /* pulsar */
        {"pulsar.address",
         CreateStringConfig("pulsar.address", false, &config.pulsar.address.value, "localhost", nullptr, nullptr)},
        {"pulsar.port",
         CreateIntegerConfig("pulsar.port", false, 0, 65535, &config.pulsar.port.value, 6650, nullptr, nullptr)},

        /* log */
        {"logs.level", CreateStringConfig("logs.level", false, &config.logs.level.value, "debug", nullptr, nullptr)},
        {"logs.trace.enable",
         CreateBoolConfig("logs.trace.enable", false, &config.logs.trace.enable.value, true, nullptr, nullptr)},
        {"logs.path",
         CreateStringConfig("logs.path", false, &config.logs.path.value, "/var/lib/milvus/logs", nullptr, nullptr)},
        {"logs.max_log_file_size", CreateSizeConfig("logs.max_log_file_size", false, 512 * MB, 4096 * MB,
                                                    &config.logs.max_log_file_size.value, 1024 * MB, nullptr, nullptr)},
        {"logs.log_rotate_num", CreateIntegerConfig("logs.log_rotate_num", false, 0, 1024,
                                                    &config.logs.log_rotate_num.value, 0, nullptr, nullptr)},

        /* tracing */
        {"tracing.json_config_path", CreateStringConfig("tracing.json_config_path", false,
                                                        &config.tracing.json_config_path.value, "", nullptr, nullptr)},

        /* invisible */
        /* engine */
        {"engine.build_index_threshold",
         CreateIntegerConfig("engine.build_index_threshold", false, 0, std::numeric_limits<int64_t>::max(),
                             &config.engine.build_index_threshold.value, 4096, nullptr, nullptr)},
        {"engine.search_combine_nq",
         CreateIntegerConfig("engine.search_combine_nq", true, 0, std::numeric_limits<int64_t>::max(),
                             &config.engine.search_combine_nq.value, 64, nullptr, nullptr)},
        {"engine.use_blas_threshold",
         CreateIntegerConfig("engine.use_blas_threshold", true, 0, std::numeric_limits<int64_t>::max(),
                             &config.engine.use_blas_threshold.value, 1100, nullptr, nullptr)},
        {"engine.omp_thread_num",
         CreateIntegerConfig("engine.omp_thread_num", true, 0, std::numeric_limits<int64_t>::max(),
                             &config.engine.omp_thread_num.value, 0, nullptr, nullptr)},
        {"engine.simd_type", CreateEnumConfig("engine.simd_type", false, &SimdMap, &config.engine.simd_type.value,
                                              SimdType::AUTO, nullptr, nullptr)},
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
ConfigMgr::Load(const std::string& path) {
    /* load from milvus.yaml */
    auto yaml = YAML::LoadFile(path);
    /* make it flattened */
    std::unordered_map<std::string, std::string> flattened;
    // auto proxy_yaml = yaml["porxy"];
    auto other_yaml = YAML::Node{};
    other_yaml["pulsar"] = yaml["pulsar"];
    Flatten(yaml["proxy"], flattened, "");
    Flatten(other_yaml, flattened, "");
    // Flatten(yaml["proxy"], flattened, "");
    /* update config */
    for (auto& it : flattened) Set(it.first, it.second, false);
}

void
ConfigMgr::Set(const std::string& name, const std::string& value, bool update) {
    std::cout << "InSet Config " << name << std::endl;
    if (config_list_.find(name) == config_list_.end()) {
        std::cout << "Config " << name << " not found!" << std::endl;
        return;
    }
    try {
        auto& config = config_list_.at(name);
        std::unique_lock<std::mutex> lock(GetConfigMutex());
        /* update=false when loading from config file */
        if (not update) {
            ThrowIfNotSuccess(config->Set(value, update));
        } else if (config->modifiable_) {
            /* set manually */
            ThrowIfNotSuccess(config->Set(value, update));
            lock.unlock();
            Notify(name);
        } else {
            throw ConfigStatus(SetReturn::IMMUTABLE, "Config " + name + " is not modifiable");
        }
    } catch (ConfigStatus& cs) {
        throw cs;
    } catch (...) {
        throw "Config " + name + " not found.";
    }
}

std::string
ConfigMgr::Get(const std::string& name) const {
    try {
        auto& config = config_list_.at(name);
        std::lock_guard<std::mutex> lock(GetConfigMutex());
        return config->Get();
    } catch (...) {
        throw "Config " + name + " not found.";
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

void
ConfigMgr::Attach(const std::string& name, ConfigObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    observers_[name].push_back(observer);
}

void
ConfigMgr::Detach(const std::string& name, ConfigObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end())
        return;
    auto& ob_list = observers_[name];
    ob_list.remove(observer);
}

void
ConfigMgr::Notify(const std::string& name) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end())
        return;
    auto& ob_list = observers_[name];
    for (auto& ob : ob_list) {
        ob->ConfigUpdate(name);
    }
}

}  // namespace milvus
