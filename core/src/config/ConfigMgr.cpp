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
#include <nlohmann/json.hpp>
#include <unordered_map>

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
        /* version */
        {"version", CreateStringConfig("version", false, &config.version.value, "unknown", nullptr, nullptr)},

        /* cluster */
        {"cluster.enable",
         CreateBoolConfig("cluster.enable", false, &config.cluster.enable.value, false, nullptr, nullptr)},
        {"cluster.role", CreateEnumConfig("cluster.role", false, &ClusterRoleMap, &config.cluster.role.value,
                                          ClusterRole::RW, nullptr, nullptr)},

        /* general */
        {"general.timezone",
         CreateStringConfig("general.timezone", false, &config.general.timezone.value, "UTC+8", nullptr, nullptr)},
        {"general.meta_uri", CreateStringConfig("general.meta_uri", false, &config.general.meta_uri.value,
                                                "sqlite://:@:/", nullptr, nullptr)},

        /* network */
        {"network.bind.address", CreateStringConfig("network.bind.address", false, &config.network.bind.address.value,
                                                    "0.0.0.0", nullptr, nullptr)},
        {"network.bind.port", CreateIntegerConfig("network.bind.port", false, 0, 65535, &config.network.bind.port.value,
                                                  19530, nullptr, nullptr)},
        {"network.http.enable",
         CreateBoolConfig("network.http.enable", false, &config.network.http.enable.value, true, nullptr, nullptr)},
        {"network.http.port", CreateIntegerConfig("network.http.port", false, 0, 65535, &config.network.http.port.value,
                                                  19121, nullptr, nullptr)},

        /* storage */
        {"storage.path",
         CreateStringConfig("storage.path", false, &config.storage.path.value, "/var/lib/milvus", nullptr, nullptr)},
        {"storage.auto_flush_interval",
         CreateIntegerConfig("storage.auto_flush_interval", true, 0, std::numeric_limits<int64_t>::max(),
                             &config.storage.auto_flush_interval.value, 1, nullptr, nullptr)},

        /* wal */
        {"wal.enable", CreateBoolConfig("wal.enable", false, &config.wal.enable.value, true, nullptr, nullptr)},
        {"wal.recovery_error_ignore",
         CreateBoolConfig("wal.recovery_error_ignore", false, &config.wal.recovery_error_ignore.value, false, nullptr,
                          nullptr)},
        {"wal.buffer_size", CreateSizeConfig("wal.buffer_size", false, 64 * MB, 4096 * MB,
                                             &config.wal.buffer_size.value, 256 * MB, nullptr, nullptr)},
        {"wal.path",
         CreateStringConfig("wal.path", false, &config.wal.path.value, "/var/lib/milvus/wal", nullptr, nullptr)},

        /* cache */
        {"cache.cache_size", CreateSizeConfig("cache.cache_size", true, 0, std::numeric_limits<int64_t>::max(),
                                              &config.cache.cache_size.value, 4 * GB, nullptr, nullptr)},
        {"cache.cpu_cache_threshold",
         CreateFloatingConfig("cache.cpu_cache_threshold", false, 0.0, 1.0, &config.cache.cpu_cache_threshold.value,
                              0.7, nullptr, nullptr)},
        {"cache.insert_buffer_size",
         CreateSizeConfig("cache.insert_buffer_size", false, 0, std::numeric_limits<int64_t>::max(),
                          &config.cache.insert_buffer_size.value, 1 * GB, nullptr, nullptr)},
        {"cache.cache_insert_data", CreateBoolConfig("cache.cache_insert_data", false,
                                                     &config.cache.cache_insert_data.value, false, nullptr, nullptr)},
        {"cache.preload_collection", CreateStringConfig("cache.preload_collection", false,
                                                        &config.cache.preload_collection.value, "", nullptr, nullptr)},

        /* gpu */
        {"gpu.enable", CreateBoolConfig("gpu.enable", false, &config.gpu.enable.value, false, nullptr, nullptr)},
        {"gpu.cache_size", CreateSizeConfig("gpu.cache_size", true, 0, std::numeric_limits<int64_t>::max(),
                                            &config.gpu.cache_size.value, 1 * GB, nullptr, nullptr)},
        {"gpu.cache_threshold", CreateFloatingConfig("gpu.cache_threshold", false, 0.0, 1.0,
                                                     &config.gpu.cache_threshold.value, 0.7, nullptr, nullptr)},
        {"gpu.gpu_search_threshold",
         CreateIntegerConfig("gpu.gpu_search_threshold", true, 0, std::numeric_limits<int64_t>::max(),
                             &config.gpu.gpu_search_threshold.value, 1000, nullptr, nullptr)},
        {"gpu.search_devices",
         CreateStringConfig("gpu.search_devices", false, &config.gpu.search_devices.value, "gpu0", nullptr, nullptr)},
        {"gpu.build_index_devices",
         CreateStringConfig("gpu.build_index_devices", false, &config.gpu.build_index_devices.value, "gpu0", nullptr,
                            nullptr)},

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

        /* metric */
        {"metric.enable",
         CreateBoolConfig("metric.enable", false, &config.metric.enable.value, false, nullptr, nullptr)},
        {"metric.address",
         CreateStringConfig("metric.address", false, &config.metric.address.value, "127.0.0.1", nullptr, nullptr)},
        {"metric.port",
         CreateIntegerConfig("metric.port", false, 1024, 65535, &config.metric.port.value, 9091, nullptr, nullptr)},

        /* tracing */
        {"tracing.json_config_path", CreateStringConfig("tracing.json_config_path", false,
                                                        &config.tracing.json_config_path.value, "", nullptr, nullptr)},

        /* invisible */
        /* engine */
        {"engine.build_index_threshold",
         CreateIntegerConfig("engine.build_index_threshold", true, 0, std::numeric_limits<int64_t>::max(),
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
        {"engine.clustering_type",
         CreateEnumConfig("engine.clustering_type", false, &ClusteringMap, &config.engine.clustering_type.value,
                          ClusteringType::K_MEANS, nullptr, nullptr)},
        {"engine.simd_type", CreateEnumConfig("engine.simd_type", false, &SimdMap, &config.engine.simd_type.value,
                                              SimdType::AUTO, nullptr, nullptr)},

        {"system.lock.enable",
         CreateBoolConfig("system.lock.enable", false, &config.system.lock.enable.value, true, nullptr, nullptr)},
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
    Flatten(yaml, flattened, "");

    /* update config */
    for (auto& it : flattened) Set(it.first, it.second, false);
}

void
ConfigMgr::Set(const std::string& name, const std::string& value, bool update) {
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
        throw;
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

std::string
ConfigMgr::JsonDump() const {
    nlohmann::json j;
    for (auto& kv : config_list_) {
        auto& config = kv.second;
        j[config->name_] = config->Get();
    }
    return j.dump();
}

void
ConfigMgr::Attach(const std::string& name, ConfigObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    observers_[name].push_back(observer);
}

void
ConfigMgr::Detach(const std::string& name, ConfigObserver* observer) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end()) {
        return;
    }
    auto& ob_list = observers_[name];
    ob_list.remove(observer);
}

void
ConfigMgr::Notify(const std::string& name) {
    std::lock_guard<std::mutex> lock(observer_mutex_);
    if (observers_.find(name) == observers_.end()) {
        return;
    }
    auto& ob_list = observers_[name];
    for (auto& ob : ob_list) {
        ob->ConfigUpdate(name);
    }
}

}  // namespace milvus
