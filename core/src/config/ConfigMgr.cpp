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
        CreateStringConfig("version", false, &config.version.value, "unknown", nullptr, nullptr),

        /* cluster */
        CreateBoolConfig("cluster.enable", false, &config.cluster.enable.value, false, nullptr, nullptr),
        CreateEnumConfig("cluster.role", false, &ClusterRoleMap, &config.cluster.role.value, ClusterRole::RW, nullptr,
                         nullptr),

        /* general */
        CreateStringConfig("general.timezone", false, &config.general.timezone.value, "UTC+8", nullptr, nullptr),
        CreateStringConfig("general.meta_uri", false, &config.general.meta_uri.value, "sqlite://:@:/", nullptr,
                           nullptr),

        /* network */
        CreateStringConfig("network.bind.address", false, &config.network.bind.address.value, "0.0.0.0", nullptr,
                           nullptr),
        CreateIntegerConfig("network.bind.port", false, 0, 65535, &config.network.bind.port.value, 19530, nullptr,
                            nullptr),
        CreateBoolConfig("network.http.enable", false, &config.network.http.enable.value, true, nullptr, nullptr),
        CreateIntegerConfig("network.http.port", false, 0, 65535, &config.network.http.port.value, 19121, nullptr,
                            nullptr),

        /* storage */
        CreateStringConfig("storage.path", false, &config.storage.path.value, "/var/lib/milvus", nullptr, nullptr),
        CreateIntegerConfig("storage.auto_flush_interval", false, 0, std::numeric_limits<int64_t>::max(),
                            &config.storage.auto_flush_interval.value, 1, nullptr, nullptr),
        CreateIntegerConfig("storage.file_cleanup_timeout", false, 0, 3600, &config.storage.file_cleanup_timeout.value,
                            10, nullptr, nullptr),

        /* wal */
        CreateBoolConfig("wal.enable", false, &config.wal.enable.value, true, nullptr, nullptr),
        CreateBoolConfig("wal.recovery_error_ignore", false, &config.wal.recovery_error_ignore.value, false, nullptr,
                         nullptr),
        CreateSizeConfig("wal.buffer_size", false, 64 * MB, 4096 * MB, &config.wal.buffer_size.value, 256 * MB, nullptr,
                         nullptr),
        CreateStringConfig("wal.path", false, &config.wal.path.value, "/var/lib/milvus/wal", nullptr, nullptr),

        /* cache */
        CreateSizeConfig("cache.cache_size", true, 0, std::numeric_limits<int64_t>::max(),
                         &config.cache.cache_size.value, 4 * GB, nullptr, nullptr),
        CreateFloatingConfig("cache.cpu_cache_threshold", false, 0.0, 1.0, &config.cache.cpu_cache_threshold.value, 0.7,
                             nullptr, nullptr),
        CreateSizeConfig("cache.insert_buffer_size", false, 0, std::numeric_limits<int64_t>::max(),
                         &config.cache.insert_buffer_size.value, 1 * GB, nullptr, nullptr),
        CreateBoolConfig("cache.cache_insert_data", false, &config.cache.cache_insert_data.value, false, nullptr,
                         nullptr),
        CreateStringConfig("cache.preload_collection", false, &config.cache.preload_collection.value, "", nullptr,
                           nullptr),

        /* gpu */
        CreateBoolConfig("gpu.enable", false, &config.gpu.enable.value, false, nullptr, nullptr),
        CreateSizeConfig("gpu.cache_size", true, 0, std::numeric_limits<int64_t>::max(), &config.gpu.cache_size.value,
                         1 * GB, nullptr, nullptr),
        CreateFloatingConfig("gpu.cache_threshold", false, 0.0, 1.0, &config.gpu.cache_threshold.value, 0.7, nullptr,
                             nullptr),
        CreateIntegerConfig("gpu.gpu_search_threshold", true, 0, std::numeric_limits<int64_t>::max(),
                            &config.gpu.gpu_search_threshold.value, 1000, nullptr, nullptr),
        CreateStringConfig("gpu.search_devices", false, &config.gpu.search_devices.value, "gpu0", nullptr, nullptr),
        CreateStringConfig("gpu.build_index_devices", false, &config.gpu.build_index_devices.value, "gpu0", nullptr,
                           nullptr),

        /* log */
        CreateStringConfig("logs.level", false, &config.logs.level.value, "debug", nullptr, nullptr),
        CreateBoolConfig("logs.trace.enable", false, &config.logs.trace.enable.value, true, nullptr, nullptr),
        CreateStringConfig("logs.path", false, &config.logs.path.value, "/var/lib/milvus/logs", nullptr, nullptr),
        CreateSizeConfig("logs.max_log_file_size", false, 512 * MB, 4096 * MB, &config.logs.max_log_file_size.value,
                         1024 * MB, nullptr, nullptr),
        CreateIntegerConfig("logs.log_rotate_num", false, 0, 1024, &config.logs.log_rotate_num.value, 0, nullptr,
                            nullptr),

        /* metric */
        CreateBoolConfig("metric.enable", false, &config.metric.enable.value, false, nullptr, nullptr),
        CreateStringConfig("metric.address", false, &config.metric.address.value, "127.0.0.1", nullptr, nullptr),
        CreateIntegerConfig("metric.port", false, 1024, 65535, &config.metric.port.value, 9091, nullptr, nullptr),

        /* tracing */
        CreateStringConfig("tracing.json_config_path", false, &config.tracing.json_config_path.value, "", nullptr,
                           nullptr),

        /* invisible */
        /* engine */
        CreateIntegerConfig("engine.search_combine_nq", true, 0, std::numeric_limits<int64_t>::max(),
                            &config.engine.search_combine_nq.value, 64, nullptr, nullptr),
        CreateIntegerConfig("engine.use_blas_threshold", true, 0, std::numeric_limits<int64_t>::max(),
                            &config.engine.use_blas_threshold.value, 1100, nullptr, nullptr),
        CreateIntegerConfig("engine.omp_thread_num", true, 0, std::numeric_limits<int64_t>::max(),
                            &config.engine.omp_thread_num.value, 0, nullptr, nullptr),
        CreateEnumConfig("engine.simd_type", false, &SimdMap, &config.engine.simd_type.value, SimdType::AUTO, nullptr,
                         nullptr),

        /* db */
        CreateFloatingConfig("db.archive_disk_threshold", false, 0.0, 1.0, &config.db.archive_disk_threshold.value, 0.0,
                             nullptr, nullptr),
        CreateIntegerConfig("db.archive_days_threshold", false, 0, std::numeric_limits<int64_t>::max(),
                            &config.db.archive_days_threshold.value, 0, nullptr, nullptr),
    };
}

void
ConfigMgr::Init() {
    std::lock_guard<std::mutex> lock(GetConfigMutex());
    for (auto& config : config_list_) config->Init();
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
    for (auto& config : config_list_) {
        if (std::strcmp(name.c_str(), config->name_) == 0) {
            std::lock_guard<std::mutex> lock(GetConfigMutex());
            if (not update || config->modifiable_) {
                ThrowIfNotSuccess(config->Set(value, update));
                Notify(name);
                return;
            }
        }
    }
    throw "Config " + name + " not found.";
}

std::string
ConfigMgr::Get(const std::string& name) const {
    for (auto& config : config_list_)
        if (std::strcmp(name.c_str(), config->name_) == 0) {
            std::lock_guard<std::mutex> lock(GetConfigMutex());
            return config->Get();
        }
    throw "Config " + name + " not found.";
}

std::string
ConfigMgr::Dump() const {
    std::stringstream ss;
    for (auto& config : config_list_) ss << config->name_ << ": " << config->Get() << std::endl;
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
