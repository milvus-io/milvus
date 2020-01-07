// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <sys/stat.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/YamlConfigMgr.h"
#include "server/Config.h"
#include "thirdparty/nlohmann/json.hpp"
#include "utils/CommonUtil.h"
#include "utils/StringHelpFunctions.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace server {

constexpr int64_t GB = 1UL << 30;

static const std::unordered_map<std::string, std::string> milvus_config_version_map({{"0.6.0", "0.1"}});

Config&
Config::GetInstance() {
    static Config config_inst;
    return config_inst;
}

Status
Config::LoadConfigFile(const std::string& filename) {
    if (filename.empty()) {
        return Status(SERVER_UNEXPECTED_ERROR, "No specified config file");
    }

    struct stat file_stat;
    if (stat(filename.c_str(), &file_stat) != 0) {
        std::string str = "Config file not exist: " + filename;
        return Status(SERVER_FILE_NOT_FOUND, str);
    }

    try {
        ConfigMgr* mgr = YamlConfigMgr::GetInstance();
        Status s = mgr->LoadConfigFile(filename);
        if (!s.ok()) {
            return s;
        }
    } catch (YAML::Exception& e) {
        std::string str = "Exception occurs when loading config file: " + filename;
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }

    return Status::OK();
}

Status
Config::ValidateConfig() {
    std::string config_version;
    CONFIG_CHECK(GetConfigVersion(config_version));

    /* server config */
    std::string server_addr;
    CONFIG_CHECK(GetServerConfigAddress(server_addr));

    std::string server_port;
    CONFIG_CHECK(GetServerConfigPort(server_port));

    std::string server_mode;
    CONFIG_CHECK(GetServerConfigDeployMode(server_mode));

    std::string server_time_zone;
    CONFIG_CHECK(GetServerConfigTimeZone(server_time_zone));

    /* db config */
    std::string db_backend_url;
    CONFIG_CHECK(GetDBConfigBackendUrl(db_backend_url));

    int64_t db_archive_disk_threshold;
    CONFIG_CHECK(GetDBConfigArchiveDiskThreshold(db_archive_disk_threshold));

    int64_t db_archive_days_threshold;
    CONFIG_CHECK(GetDBConfigArchiveDaysThreshold(db_archive_days_threshold));

    int64_t db_insert_buffer_size;
    CONFIG_CHECK(GetDBConfigInsertBufferSize(db_insert_buffer_size));

    /* storage config */
    std::string storage_primary_path;
    CONFIG_CHECK(GetStorageConfigPrimaryPath(storage_primary_path));

    std::string storage_secondary_path;
    CONFIG_CHECK(GetStorageConfigSecondaryPath(storage_secondary_path));

    bool storage_minio_enable;
    CONFIG_CHECK(GetStorageConfigMinioEnable(storage_minio_enable));
    std::cout << "MinIO " << (storage_minio_enable ? "ENABLED !" : "DISABLED !") << std::endl;

    std::string storage_minio_address;
    CONFIG_CHECK(GetStorageConfigMinioAddress(storage_minio_address));

    std::string storage_minio_port;
    CONFIG_CHECK(GetStorageConfigMinioPort(storage_minio_port));

    std::string storage_minio_access_key;
    CONFIG_CHECK(GetStorageConfigMinioAccessKey(storage_minio_access_key));

    std::string storage_minio_secret_key;
    CONFIG_CHECK(GetStorageConfigMinioSecretKey(storage_minio_secret_key));

    std::string storage_minio_bucket;
    CONFIG_CHECK(GetStorageConfigMinioBucket(storage_minio_bucket));

    /* metric config */
    bool metric_enable_monitor;
    CONFIG_CHECK(GetMetricConfigEnableMonitor(metric_enable_monitor));

    std::string metric_collector;
    CONFIG_CHECK(GetMetricConfigCollector(metric_collector));

    std::string metric_prometheus_port;
    CONFIG_CHECK(GetMetricConfigPrometheusPort(metric_prometheus_port));

    /* cache config */
    int64_t cache_cpu_cache_capacity;
    CONFIG_CHECK(GetCacheConfigCpuCacheCapacity(cache_cpu_cache_capacity));

    float cache_cpu_cache_threshold;
    CONFIG_CHECK(GetCacheConfigCpuCacheThreshold(cache_cpu_cache_threshold));

    bool cache_insert_data;
    CONFIG_CHECK(GetCacheConfigCacheInsertData(cache_insert_data));

    /* engine config */
    int64_t engine_use_blas_threshold;
    CONFIG_CHECK(GetEngineConfigUseBlasThreshold(engine_use_blas_threshold));

    int64_t engine_omp_thread_num;
    CONFIG_CHECK(GetEngineConfigOmpThreadNum(engine_omp_thread_num));

#ifdef MILVUS_GPU_VERSION
    int64_t engine_gpu_search_threshold;
    CONFIG_CHECK(GetEngineConfigGpuSearchThreshold(engine_gpu_search_threshold));
#endif

    /* gpu resource config */
#ifdef MILVUS_GPU_VERSION
    bool gpu_resource_enable;
    CONFIG_CHECK(GetGpuResourceConfigEnable(gpu_resource_enable));
    std::cout << "GPU resources " << (gpu_resource_enable ? "ENABLED !" : "DISABLED !") << std::endl;

    if (gpu_resource_enable) {
        int64_t resource_cache_capacity;
        CONFIG_CHECK(GetGpuResourceConfigCacheCapacity(resource_cache_capacity));

        float resource_cache_threshold;
        CONFIG_CHECK(GetGpuResourceConfigCacheThreshold(resource_cache_threshold));

        std::vector<int64_t> search_resources;
        CONFIG_CHECK(GetGpuResourceConfigSearchResources(search_resources));

        std::vector<int64_t> index_build_resources;
        CONFIG_CHECK(GetGpuResourceConfigBuildIndexResources(index_build_resources));
    }
#endif

    /* tracing config */
    std::string tracing_config_path;
    CONFIG_CHECK(GetTracingConfigJsonConfigPath(tracing_config_path));

    return Status::OK();
}

Status
Config::ResetDefaultConfig() {
    /* server config */
    CONFIG_CHECK(SetServerConfigAddress(CONFIG_SERVER_ADDRESS_DEFAULT));
    CONFIG_CHECK(SetServerConfigPort(CONFIG_SERVER_PORT_DEFAULT));
    CONFIG_CHECK(SetServerConfigDeployMode(CONFIG_SERVER_DEPLOY_MODE_DEFAULT));
    CONFIG_CHECK(SetServerConfigTimeZone(CONFIG_SERVER_TIME_ZONE_DEFAULT));

    /* db config */
    CONFIG_CHECK(SetDBConfigBackendUrl(CONFIG_DB_BACKEND_URL_DEFAULT));
    CONFIG_CHECK(SetDBConfigArchiveDiskThreshold(CONFIG_DB_ARCHIVE_DISK_THRESHOLD_DEFAULT));
    CONFIG_CHECK(SetDBConfigArchiveDaysThreshold(CONFIG_DB_ARCHIVE_DAYS_THRESHOLD_DEFAULT));
    CONFIG_CHECK(SetDBConfigInsertBufferSize(CONFIG_DB_INSERT_BUFFER_SIZE_DEFAULT));

    /* storage config */
    CONFIG_CHECK(SetStorageConfigPrimaryPath(CONFIG_STORAGE_PRIMARY_PATH_DEFAULT));
    CONFIG_CHECK(SetStorageConfigSecondaryPath(CONFIG_STORAGE_SECONDARY_PATH_DEFAULT));
    CONFIG_CHECK(SetStorageConfigMinioEnable(CONFIG_STORAGE_MINIO_ENABLE_DEFAULT));
    CONFIG_CHECK(SetStorageConfigMinioAddress(CONFIG_STORAGE_MINIO_ADDRESS_DEFAULT));
    CONFIG_CHECK(SetStorageConfigMinioPort(CONFIG_STORAGE_MINIO_PORT_DEFAULT));
    CONFIG_CHECK(SetStorageConfigMinioAccessKey(CONFIG_STORAGE_MINIO_ACCESS_KEY_DEFAULT));
    CONFIG_CHECK(SetStorageConfigMinioSecretKey(CONFIG_STORAGE_MINIO_SECRET_KEY_DEFAULT));
    CONFIG_CHECK(SetStorageConfigMinioBucket(CONFIG_STORAGE_MINIO_BUCKET_DEFAULT));

    /* metric config */
    CONFIG_CHECK(SetMetricConfigEnableMonitor(CONFIG_METRIC_ENABLE_MONITOR_DEFAULT));
    CONFIG_CHECK(SetMetricConfigCollector(CONFIG_METRIC_COLLECTOR_DEFAULT));
    CONFIG_CHECK(SetMetricConfigPrometheusPort(CONFIG_METRIC_PROMETHEUS_PORT_DEFAULT));

    /* cache config */
    CONFIG_CHECK(SetCacheConfigCpuCacheCapacity(CONFIG_CACHE_CPU_CACHE_CAPACITY_DEFAULT));
    CONFIG_CHECK(SetCacheConfigCpuCacheThreshold(CONFIG_CACHE_CPU_CACHE_THRESHOLD_DEFAULT));
    CONFIG_CHECK(SetCacheConfigCacheInsertData(CONFIG_CACHE_CACHE_INSERT_DATA_DEFAULT));

    /* engine config */
    CONFIG_CHECK(SetEngineConfigUseBlasThreshold(CONFIG_ENGINE_USE_BLAS_THRESHOLD_DEFAULT));
    CONFIG_CHECK(SetEngineConfigOmpThreadNum(CONFIG_ENGINE_OMP_THREAD_NUM_DEFAULT));
#ifdef MILVUS_GPU_VERSION
    CONFIG_CHECK(SetEngineConfigGpuSearchThreshold(CONFIG_ENGINE_GPU_SEARCH_THRESHOLD_DEFAULT));
#endif

    /* gpu resource config */
#ifdef MILVUS_GPU_VERSION
    CONFIG_CHECK(SetGpuResourceConfigEnable(CONFIG_GPU_RESOURCE_ENABLE_DEFAULT));
    CONFIG_CHECK(SetGpuResourceConfigCacheCapacity(CONFIG_GPU_RESOURCE_CACHE_CAPACITY_DEFAULT));
    CONFIG_CHECK(SetGpuResourceConfigCacheThreshold(CONFIG_GPU_RESOURCE_CACHE_THRESHOLD_DEFAULT));
    CONFIG_CHECK(SetGpuResourceConfigSearchResources(CONFIG_GPU_RESOURCE_SEARCH_RESOURCES_DEFAULT));
    CONFIG_CHECK(SetGpuResourceConfigBuildIndexResources(CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES_DEFAULT));
#endif

    return Status::OK();
}

void
Config::GetConfigJsonStr(std::string& result) {
    nlohmann::json config_json(config_map_);
    result = config_json.dump();
}

Status
Config::GetConfigCli(std::string& value, const std::string& parent_key, const std::string& child_key) {
    if (!ConfigNodeValid(parent_key, child_key)) {
        std::string str = "Config node invalid: " + parent_key + CONFIG_NODE_DELIMITER + child_key;
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }
    return GetConfigValueInMem(parent_key, child_key, value);
}

Status
Config::SetConfigCli(const std::string& parent_key, const std::string& child_key, const std::string& value) {
    if (!ConfigNodeValid(parent_key, child_key)) {
        std::string str = "Config node invalid: " + parent_key + CONFIG_NODE_DELIMITER + child_key;
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }
    if (parent_key == CONFIG_SERVER) {
        return Status(SERVER_UNSUPPORTED_ERROR, "Not support set server_config");
    } else if (parent_key == CONFIG_DB) {
        return Status(SERVER_UNSUPPORTED_ERROR, "Not support set db_config");
    } else if (parent_key == CONFIG_STORAGE) {
        return Status(SERVER_UNSUPPORTED_ERROR, "Not support set storage_config");
    } else if (parent_key == CONFIG_METRIC) {
        return Status(SERVER_UNSUPPORTED_ERROR, "Not support set metric_config");
    } else if (parent_key == CONFIG_CACHE) {
        if (child_key == CONFIG_CACHE_CPU_CACHE_CAPACITY) {
            return SetCacheConfigCpuCacheCapacity(value);
        } else if (child_key == CONFIG_CACHE_CPU_CACHE_THRESHOLD) {
            return SetCacheConfigCpuCacheThreshold(value);
        } else if (child_key == CONFIG_CACHE_CACHE_INSERT_DATA) {
            return SetCacheConfigCacheInsertData(value);
        }
    } else if (parent_key == CONFIG_ENGINE) {
        if (child_key == CONFIG_ENGINE_USE_BLAS_THRESHOLD) {
            return SetEngineConfigUseBlasThreshold(value);
        } else if (child_key == CONFIG_ENGINE_OMP_THREAD_NUM) {
            return SetEngineConfigOmpThreadNum(value);
#ifdef MILVUS_GPU_VERSION
        } else if (child_key == CONFIG_ENGINE_GPU_SEARCH_THRESHOLD) {
            return SetEngineConfigGpuSearchThreshold(value);
#endif
        }
#ifdef MILVUS_GPU_VERSION
    } else if (parent_key == CONFIG_GPU_RESOURCE) {
        if (child_key == CONFIG_GPU_RESOURCE_ENABLE) {
            return SetGpuResourceConfigEnable(value);
        } else if (child_key == CONFIG_GPU_RESOURCE_CACHE_CAPACITY) {
            return SetGpuResourceConfigCacheCapacity(value);
        } else if (child_key == CONFIG_GPU_RESOURCE_CACHE_THRESHOLD) {
            return SetGpuResourceConfigCacheThreshold(value);
        } else if (child_key == CONFIG_GPU_RESOURCE_SEARCH_RESOURCES) {
            return SetGpuResourceConfigSearchResources(value);
        } else if (child_key == CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES) {
            return SetGpuResourceConfigBuildIndexResources(value);
        }
#endif
    } else if (parent_key == CONFIG_TRACING) {
        return Status(SERVER_UNSUPPORTED_ERROR, "Not support set tracing_config");
    }
}

Status
Config::ProcessConfigCli(std::string& result, const std::string& cmd) {
    std::vector<std::string> tokens;
    std::vector<std::string> nodes;
    server::StringHelpFunctions::SplitStringByDelimeter(cmd, " ", tokens);
    if (tokens[0] == "get_config") {
        if (tokens.size() != 2) {
            return Status(SERVER_UNEXPECTED_ERROR, "Invalid command: " + cmd);
        }
        if (tokens[1] == "*") {
            GetConfigJsonStr(result);
            return Status::OK();
        } else {
            server::StringHelpFunctions::SplitStringByDelimeter(tokens[1], CONFIG_NODE_DELIMITER, nodes);
            if (nodes.size() != 2) {
                return Status(SERVER_UNEXPECTED_ERROR, "Invalid command: " + cmd);
            }
            return GetConfigCli(result, nodes[0], nodes[1]);
        }
    } else if (tokens[0] == "set_config") {
        if (tokens.size() != 3) {
            return Status(SERVER_UNEXPECTED_ERROR, "Invalid command: " + cmd);
        }
        server::StringHelpFunctions::SplitStringByDelimeter(tokens[1], CONFIG_NODE_DELIMITER, nodes);
        if (nodes.size() != 2) {
            return Status(SERVER_UNEXPECTED_ERROR, "Invalid command: " + cmd);
        }
        return SetConfigCli(nodes[0], nodes[1], tokens[2]);
    } else {
        return Status(SERVER_UNEXPECTED_ERROR, "Invalid command: " + cmd);
    }
}

////////////////////////////////////////////////////////////////////////////////
Status
Config::CheckConfigVersion(const std::string& value) {
    if (milvus_config_version_map.at(MILVUS_VERSION) != value) {
        std::string msg = "Invalid config version: " + value +
                          ". Expected config version: " + milvus_config_version_map.at(MILVUS_VERSION);
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

/* server config */
Status
Config::CheckServerConfigAddress(const std::string& value) {
    if (!ValidationUtil::ValidateIpAddress(value).ok()) {
        std::string msg =
            "Invalid server IP address: " + value + ". Possible reason: server_config.address is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckServerConfigPort(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid server port: " + value + ". Possible reason: server_config.port is not a number.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int32_t port = std::stoi(value);
        if (!(port > 1024 && port < 65535)) {
            std::string msg = "Invalid server port: " + value +
                              ". Possible reason: server_config.port is not in range (1024, 65535).";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

Status
Config::CheckServerConfigDeployMode(const std::string& value) {
    if (value != "single" && value != "cluster_readonly" && value != "cluster_writable") {
        return Status(SERVER_INVALID_ARGUMENT,
                      "server_config.deploy_mode is not one of single, cluster_readonly, and cluster_writable.");
    }
    return Status::OK();
}

Status
Config::CheckServerConfigTimeZone(const std::string& value) {
    if (value.length() <= 3) {
        return Status(SERVER_INVALID_ARGUMENT, "Invalid server_config.time_zone: " + value);
    } else {
        if (value.substr(0, 3) != "UTC") {
            return Status(SERVER_INVALID_ARGUMENT, "Invalid server_config.time_zone: " + value);
        } else {
            try {
                stoi(value.substr(3));
            } catch (...) {
                return Status(SERVER_INVALID_ARGUMENT, "Invalid server_config.time_zone: " + value);
            }
        }
    }
    return Status::OK();
}

/* DB config */
Status
Config::CheckDBConfigBackendUrl(const std::string& value) {
    if (!ValidationUtil::ValidateDbURI(value).ok()) {
        std::string msg =
            "Invalid backend url: " + value + ". Possible reason: db_config.db_backend_url is invalid. " +
            "The correct format should be like sqlite://:@:/ or mysql://root:123456@127.0.0.1:3306/milvus.";
        return Status(SERVER_INVALID_ARGUMENT, "invalid db_backend_url: " + value);
    }
    return Status::OK();
}

Status
Config::CheckDBConfigArchiveDiskThreshold(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid archive disk threshold: " + value +
                          ". Possible reason: db_config.archive_disk_threshold is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckDBConfigArchiveDaysThreshold(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid archive days threshold: " + value +
                          ". Possible reason: db_config.archive_days_threshold is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckDBConfigInsertBufferSize(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid insert buffer size: " + value +
                          ". Possible reason: db_config.insert_buffer_size is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int64_t buffer_size = std::stoll(value) * GB;
        if (buffer_size <= 0) {
            std::string msg = "Invalid insert buffer size: " + value +
                              ". Possible reason: db_config.insert_buffer_size is not a positive integer.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }

        uint64_t total_mem = 0, free_mem = 0;
        CommonUtil::GetSystemMemInfo(total_mem, free_mem);
        if (buffer_size >= total_mem) {
            std::string msg = "Invalid insert buffer size: " + value +
                              ". Possible reason: db_config.insert_buffer_size exceeds system memory.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

/* storage config */
Status
Config::CheckStorageConfigPrimaryPath(const std::string& value) {
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.db_path is empty.");
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigSecondaryPath(const std::string& value) {
    return Status::OK();
}

Status
Config::CheckStorageConfigMinioEnable(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsBool(value).ok()) {
        std::string msg =
            "Invalid storage config: " + value + ". Possible reason: storage_config.minio_enable is not a boolean.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigMinioAddress(const std::string& value) {
    if (!ValidationUtil::ValidateIpAddress(value).ok()) {
        std::string msg =
            "Invalid minio address: " + value + ". Possible reason: storage_config.minio_address is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigMinioPort(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid minio port: " + value + ". Possible reason: storage_config.port is not a number.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int32_t port = std::stoi(value);
        if (!(port > 1024 && port < 65535)) {
            std::string msg = "Invalid minio port: " + value +
                              ". Possible reason: storage_config.port is not in range (1024, 65535).";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigMinioAccessKey(const std::string& value) {
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.minio_access_key is empty.");
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigMinioSecretKey(const std::string& value) {
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.minio_secret_key is empty.");
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigMinioBucket(const std::string& value) {
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.minio_bucket is empty.");
    }
    return Status::OK();
}

/* metric config */
Status
Config::CheckMetricConfigEnableMonitor(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsBool(value).ok()) {
        std::string msg =
            "Invalid metric config: " + value + ". Possible reason: metric_config.enable_monitor is not a boolean.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckMetricConfigCollector(const std::string& value) {
    if (value != "prometheus") {
        std::string msg =
            "Invalid metric collector: " + value + ". Possible reason: metric_config.collector is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckMetricConfigPrometheusPort(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid prometheus port: " + value +
                          ". Possible reason: metric_config.prometheus_config.port is not a number.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int32_t port = std::stoi(value);
        if (!(port > 1024 && port < 65535)) {
            std::string msg = "Invalid prometheus port: " + value +
                              ". Possible reason: metric_config.prometheus_config.port is not in range (1024, 65535).";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

/* cache config */
Status
Config::CheckCacheConfigCpuCacheCapacity(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid cpu cache capacity: " + value +
                          ". Possible reason: cache_config.cpu_cache_capacity is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int64_t cpu_cache_capacity = std::stoll(value) * GB;
        if (cpu_cache_capacity <= 0) {
            std::string msg = "Invalid cpu cache capacity: " + value +
                              ". Possible reason: cache_config.cpu_cache_capacity is not a positive integer.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }

        uint64_t total_mem = 0, free_mem = 0;
        CommonUtil::GetSystemMemInfo(total_mem, free_mem);
        if (static_cast<uint64_t>(cpu_cache_capacity) >= total_mem) {
            std::string msg = "Invalid cpu cache capacity: " + value +
                              ". Possible reason: cache_config.cpu_cache_capacity exceeds system memory.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        } else if (static_cast<double>(cpu_cache_capacity) > static_cast<double>(total_mem * 0.9)) {
            std::cerr << "WARNING: cpu cache capacity value is too big" << std::endl;
        }

        int64_t buffer_value;
        CONFIG_CHECK(GetDBConfigInsertBufferSize(buffer_value));

        int64_t insert_buffer_size = buffer_value * GB;
//        if (insert_buffer_size + cpu_cache_capacity >= total_mem) {
//            std::string msg = "Invalid cpu cache capacity: " + value +
//                              ". Possible reason: sum of cache_config.cpu_cache_capacity and "
//                              "db_config.insert_buffer_size exceeds system memory.";
//            return Status(SERVER_INVALID_ARGUMENT, msg);
//        }
    }
    return Status::OK();
}

Status
Config::CheckCacheConfigCpuCacheThreshold(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsFloat(value).ok()) {
        std::string msg = "Invalid cpu cache threshold: " + value +
                          ". Possible reason: cache_config.cpu_cache_threshold is not in range (0.0, 1.0].";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        float cpu_cache_threshold = std::stof(value);
        if (cpu_cache_threshold <= 0.0 || cpu_cache_threshold >= 1.0) {
            std::string msg = "Invalid cpu cache threshold: " + value +
                              ". Possible reason: cache_config.cpu_cache_threshold is not in range (0.0, 1.0].";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

Status
Config::CheckCacheConfigCacheInsertData(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsBool(value).ok()) {
        std::string msg = "Invalid cache insert data option: " + value +
                          ". Possible reason: cache_config.cache_insert_data is not a boolean.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

/* engine config */
Status
Config::CheckEngineConfigUseBlasThreshold(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid use blas threshold: " + value +
                          ". Possible reason: engine_config.use_blas_threshold is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckEngineConfigOmpThreadNum(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid omp thread num: " + value +
                          ". Possible reason: engine_config.omp_thread_num is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    int64_t omp_thread = std::stoll(value);
    int64_t sys_thread_cnt = 8;
    CommonUtil::GetSystemAvailableThreads(sys_thread_cnt);
    if (omp_thread > sys_thread_cnt) {
        std::string msg = "Invalid omp thread num: " + value +
                          ". Possible reason: engine_config.omp_thread_num exceeds system cpu cores.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

#ifdef MILVUS_GPU_VERSION
Status
Config::CheckEngineConfigGpuSearchThreshold(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid gpu search threshold: " + value +
                          ". Possible reason: engine_config.gpu_search_threshold is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

/* gpu resource config */
Status
Config::CheckGpuResourceConfigEnable(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsBool(value).ok()) {
        std::string msg =
            "Invalid gpu resource config: " + value + ". Possible reason: gpu_resource_config.enable is not a boolean.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckGpuResourceConfigCacheCapacity(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid gpu cache capacity: " + value +
                          ". Possible reason: gpu_resource_config.cache_capacity is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int64_t gpu_cache_capacity = std::stoll(value) * GB;
        std::vector<int64_t> gpu_ids;
        CONFIG_CHECK(GetGpuResourceConfigBuildIndexResources(gpu_ids));

        for (int64_t gpu_id : gpu_ids) {
            size_t gpu_memory;
            if (!ValidationUtil::GetGpuMemory(gpu_id, gpu_memory).ok()) {
                std::string msg = "Fail to get GPU memory for GPU device: " + std::to_string(gpu_id);
                return Status(SERVER_UNEXPECTED_ERROR, msg);
            } else if (gpu_cache_capacity >= gpu_memory) {
                std::string msg = "Invalid gpu cache capacity: " + value +
                                  ". Possible reason: gpu_resource_config.cache_capacity exceeds GPU memory.";
                return Status(SERVER_INVALID_ARGUMENT, msg);
            } else if (gpu_cache_capacity > (double)gpu_memory * 0.9) {
                std::cerr << "Warning: gpu cache capacity value is too big" << std::endl;
            }
        }
    }
    return Status::OK();
}

Status
Config::CheckGpuResourceConfigCacheThreshold(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsFloat(value).ok()) {
        std::string msg = "Invalid gpu cache threshold: " + value +
                          ". Possible reason: gpu_resource_config.cache_threshold is not in range (0.0, 1.0].";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        float gpu_cache_threshold = std::stof(value);
        if (gpu_cache_threshold <= 0.0 || gpu_cache_threshold >= 1.0) {
            std::string msg = "Invalid gpu cache threshold: " + value +
                              ". Possible reason: gpu_resource_config.cache_threshold is not in range (0.0, 1.0].";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

Status
CheckGpuResource(const std::string& value) {
    std::string s = value;
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);

    const std::regex pat("gpu(\\d+)");
    std::smatch m;
    if (!std::regex_match(s, m, pat)) {
        std::string msg = "Invalid gpu resource: " + value +
                          ". Possible reason: gpu_resource_config is not in the format of cpux or gpux";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    if (s.compare(0, 3, "gpu") == 0) {
        int32_t gpu_index = std::stoi(s.substr(3));
        if (!ValidationUtil::ValidateGpuIndex(gpu_index).ok()) {
            std::string msg = "Invalid gpu resource: " + value +
                              ". Possible reason: gpu_resource_config does not match with the hardware.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }

    return Status::OK();
}

Status
Config::CheckGpuResourceConfigSearchResources(const std::vector<std::string>& value) {
    if (value.empty()) {
        std::string msg =
            "Invalid gpu search resource. "
            "Possible reason: gpu_resource_config.search_resources is empty.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    for (auto& resource : value) {
        CONFIG_CHECK(CheckGpuResource(resource));
    }
    return Status::OK();
}

Status
Config::CheckGpuResourceConfigBuildIndexResources(const std::vector<std::string>& value) {
    if (value.empty()) {
        std::string msg =
            "Invalid gpu build index resource. "
            "Possible reason: gpu_resource_config.build_index_resources is empty.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    for (auto& resource : value) {
        CONFIG_CHECK(CheckGpuResource(resource));
    }

    return Status::OK();
}
#endif

////////////////////////////////////////////////////////////////////////////////
ConfigNode&
Config::GetConfigRoot() {
    ConfigMgr* mgr = YamlConfigMgr::GetInstance();
    return mgr->GetRootNode();
}

ConfigNode&
Config::GetConfigNode(const std::string& name) {
    return GetConfigRoot().GetChild(name);
}

bool
Config::ConfigNodeValid(const std::string& parent_key, const std::string& child_key) {
    if (config_map_.find(parent_key) == config_map_.end()) {
        return false;
    }
    if (config_map_[parent_key].count(child_key) == 0) {
        return false;
    }
    return true;
}

Status
Config::GetConfigValueInMem(const std::string& parent_key, const std::string& child_key, std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (config_map_.find(parent_key) != config_map_.end() &&
        config_map_[parent_key].find(child_key) != config_map_[parent_key].end()) {
        value = config_map_[parent_key][child_key];
        return Status::OK();
    }
    return Status(SERVER_UNEXPECTED_ERROR, "key not exist");
}

Status
Config::SetConfigValueInMem(const std::string& parent_key, const std::string& child_key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    config_map_[parent_key][child_key] = value;
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
std::string
Config::GetConfigStr(const std::string& parent_key, const std::string& child_key, const std::string& default_value) {
    std::string value;
    if (!GetConfigValueInMem(parent_key, child_key, value).ok()) {
        value = GetConfigNode(parent_key).GetValue(child_key, default_value);
        SetConfigValueInMem(parent_key, child_key, value);
    }
    return value;
}

std::string
Config::GetConfigSequenceStr(const std::string& parent_key, const std::string& child_key, const std::string& delim,
                             const std::string& default_value) {
    std::string value;
    if (!GetConfigValueInMem(parent_key, child_key, value).ok()) {
        std::vector<std::string> sequence = GetConfigNode(parent_key).GetSequence(child_key);
        if (sequence.empty()) {
            value = default_value;
        } else {
            server::StringHelpFunctions::MergeStringWithDelimeter(sequence, delim, value);
        }
        SetConfigValueInMem(parent_key, child_key, value);
    }
    return value;
}

Status
Config::GetConfigVersion(std::string& value) {
    value = GetConfigRoot().GetValue(CONFIG_VERSION);
    return CheckConfigVersion(value);
}

/* server config */
Status
Config::GetServerConfigAddress(std::string& value) {
    value = GetConfigStr(CONFIG_SERVER, CONFIG_SERVER_ADDRESS, CONFIG_SERVER_ADDRESS_DEFAULT);
    return CheckServerConfigAddress(value);
}

Status
Config::GetServerConfigPort(std::string& value) {
    value = GetConfigStr(CONFIG_SERVER, CONFIG_SERVER_PORT, CONFIG_SERVER_PORT_DEFAULT);
    return CheckServerConfigPort(value);
}

Status
Config::GetServerConfigDeployMode(std::string& value) {
    value = GetConfigStr(CONFIG_SERVER, CONFIG_SERVER_DEPLOY_MODE, CONFIG_SERVER_DEPLOY_MODE_DEFAULT);
    return CheckServerConfigDeployMode(value);
}

Status
Config::GetServerConfigTimeZone(std::string& value) {
    value = GetConfigStr(CONFIG_SERVER, CONFIG_SERVER_TIME_ZONE, CONFIG_SERVER_TIME_ZONE_DEFAULT);
    return CheckServerConfigTimeZone(value);
}

/* DB config */
Status
Config::GetDBConfigBackendUrl(std::string& value) {
    value = GetConfigStr(CONFIG_DB, CONFIG_DB_BACKEND_URL, CONFIG_DB_BACKEND_URL_DEFAULT);
    return CheckDBConfigBackendUrl(value);
}

Status
Config::GetDBConfigArchiveDiskThreshold(int64_t& value) {
    std::string str =
        GetConfigStr(CONFIG_DB, CONFIG_DB_ARCHIVE_DISK_THRESHOLD, CONFIG_DB_ARCHIVE_DISK_THRESHOLD_DEFAULT);
    CONFIG_CHECK(CheckDBConfigArchiveDiskThreshold(str));
    value = std::stoll(str);
    return Status::OK();
}

Status
Config::GetDBConfigArchiveDaysThreshold(int64_t& value) {
    std::string str =
        GetConfigStr(CONFIG_DB, CONFIG_DB_ARCHIVE_DAYS_THRESHOLD, CONFIG_DB_ARCHIVE_DAYS_THRESHOLD_DEFAULT);
    CONFIG_CHECK(CheckDBConfigArchiveDaysThreshold(str));
    value = std::stoll(str);
    return Status::OK();
}

Status
Config::GetDBConfigInsertBufferSize(int64_t& value) {
    std::string str = GetConfigStr(CONFIG_DB, CONFIG_DB_INSERT_BUFFER_SIZE, CONFIG_DB_INSERT_BUFFER_SIZE_DEFAULT);
    CONFIG_CHECK(CheckDBConfigInsertBufferSize(str));
    value = std::stoll(str);
    return Status::OK();
}

Status
Config::GetDBConfigPreloadTable(std::string& value) {
    value = GetConfigStr(CONFIG_DB, CONFIG_DB_PRELOAD_TABLE);
    return Status::OK();
}

/* storage config */
Status
Config::GetStorageConfigPrimaryPath(std::string& value) {
    value = GetConfigStr(CONFIG_DB, CONFIG_STORAGE_PRIMARY_PATH, CONFIG_STORAGE_PRIMARY_PATH_DEFAULT);
    return CheckStorageConfigPrimaryPath(value);
}

Status
Config::GetStorageConfigSecondaryPath(std::string& value) {
    value = GetConfigStr(CONFIG_DB, CONFIG_STORAGE_SECONDARY_PATH, CONFIG_STORAGE_SECONDARY_PATH_DEFAULT);
    return CheckStorageConfigSecondaryPath(value);
}

Status
Config::GetStorageConfigMinioEnable(bool& value) {
    std::string str = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_ENABLE, CONFIG_STORAGE_MINIO_ENABLE_DEFAULT);
    CONFIG_CHECK(CheckStorageConfigMinioEnable(str));
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    value = (str == "true" || str == "on" || str == "yes" || str == "1");
    return Status::OK();
}

Status
Config::GetStorageConfigMinioAddress(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_ADDRESS, CONFIG_STORAGE_MINIO_ADDRESS_DEFAULT);
    return CheckStorageConfigMinioAddress(value);
}

Status
Config::GetStorageConfigMinioPort(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_PORT, CONFIG_STORAGE_MINIO_PORT_DEFAULT);
    return CheckStorageConfigMinioPort(value);
}

Status
Config::GetStorageConfigMinioAccessKey(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_ACCESS_KEY, CONFIG_STORAGE_MINIO_ACCESS_KEY_DEFAULT);
    return Status::OK();
}

Status
Config::GetStorageConfigMinioSecretKey(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_SECRET_KEY, CONFIG_STORAGE_MINIO_SECRET_KEY_DEFAULT);
    return Status::OK();
}

Status
Config::GetStorageConfigMinioBucket(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_BUCKET, CONFIG_STORAGE_MINIO_BUCKET_DEFAULT);
    return Status::OK();
}

/* metric config */
Status
Config::GetMetricConfigEnableMonitor(bool& value) {
    std::string str = GetConfigStr(CONFIG_METRIC, CONFIG_METRIC_ENABLE_MONITOR, CONFIG_METRIC_ENABLE_MONITOR_DEFAULT);
    CONFIG_CHECK(CheckMetricConfigEnableMonitor(str));
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    value = (str == "true" || str == "on" || str == "yes" || str == "1");
    return Status::OK();
}

Status
Config::GetMetricConfigCollector(std::string& value) {
    value = GetConfigStr(CONFIG_METRIC, CONFIG_METRIC_COLLECTOR, CONFIG_METRIC_COLLECTOR_DEFAULT);
    return Status::OK();
}

Status
Config::GetMetricConfigPrometheusPort(std::string& value) {
    value = GetConfigStr(CONFIG_METRIC, CONFIG_METRIC_PROMETHEUS_PORT, CONFIG_METRIC_PROMETHEUS_PORT_DEFAULT);
    return CheckMetricConfigPrometheusPort(value);
}

/* cache config */
Status
Config::GetCacheConfigCpuCacheCapacity(int64_t& value) {
    std::string str =
        GetConfigStr(CONFIG_CACHE, CONFIG_CACHE_CPU_CACHE_CAPACITY, CONFIG_CACHE_CPU_CACHE_CAPACITY_DEFAULT);
    CONFIG_CHECK(CheckCacheConfigCpuCacheCapacity(str));
    value = std::stoll(str);
    return Status::OK();
}

Status
Config::GetCacheConfigCpuCacheThreshold(float& value) {
    std::string str =
        GetConfigStr(CONFIG_CACHE, CONFIG_CACHE_CPU_CACHE_THRESHOLD, CONFIG_CACHE_CPU_CACHE_THRESHOLD_DEFAULT);
    CONFIG_CHECK(CheckCacheConfigCpuCacheThreshold(str));
    value = std::stof(str);
    return Status::OK();
}

Status
Config::GetCacheConfigCacheInsertData(bool& value) {
    std::string str =
        GetConfigStr(CONFIG_CACHE, CONFIG_CACHE_CACHE_INSERT_DATA, CONFIG_CACHE_CACHE_INSERT_DATA_DEFAULT);
    CONFIG_CHECK(CheckCacheConfigCacheInsertData(str));
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    value = (str == "true" || str == "on" || str == "yes" || str == "1");
    return Status::OK();
}

/* engine config */
Status
Config::GetEngineConfigUseBlasThreshold(int64_t& value) {
    std::string str =
        GetConfigStr(CONFIG_ENGINE, CONFIG_ENGINE_USE_BLAS_THRESHOLD, CONFIG_ENGINE_USE_BLAS_THRESHOLD_DEFAULT);
    CONFIG_CHECK(CheckEngineConfigUseBlasThreshold(str));
    value = std::stoll(str);
    return Status::OK();
}

Status
Config::GetEngineConfigOmpThreadNum(int64_t& value) {
    std::string str = GetConfigStr(CONFIG_ENGINE, CONFIG_ENGINE_OMP_THREAD_NUM, CONFIG_ENGINE_OMP_THREAD_NUM_DEFAULT);
    CONFIG_CHECK(CheckEngineConfigOmpThreadNum(str));
    value = std::stoll(str);
    return Status::OK();
}

#ifdef MILVUS_GPU_VERSION
Status
Config::GetEngineConfigGpuSearchThreshold(int64_t& value) {
    std::string str =
        GetConfigStr(CONFIG_ENGINE, CONFIG_ENGINE_GPU_SEARCH_THRESHOLD, CONFIG_ENGINE_GPU_SEARCH_THRESHOLD_DEFAULT);
    CONFIG_CHECK(CheckEngineConfigGpuSearchThreshold(str));
    value = std::stoll(str);
    return Status::OK();
}
#endif

/* gpu resource config */
#ifdef MILVUS_GPU_VERSION
Status
Config::GetGpuResourceConfigEnable(bool& value) {
    std::string str = GetConfigStr(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_ENABLE, CONFIG_GPU_RESOURCE_ENABLE_DEFAULT);
    CONFIG_CHECK(CheckGpuResourceConfigEnable(str));
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    value = (str == "true" || str == "on" || str == "yes" || str == "1");
    return Status::OK();
}

Status
Config::GetGpuResourceConfigCacheCapacity(int64_t& value) {
    bool gpu_resource_enable = false;
    CONFIG_CHECK(GetGpuResourceConfigEnable(gpu_resource_enable));
    if (!gpu_resource_enable) {
        std::string msg = "GPU not supported. Possible reason: gpu_resource_config.enable is set to false.";
        return Status(SERVER_UNSUPPORTED_ERROR, msg);
    }
    std::string str = GetConfigStr(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_CACHE_CAPACITY,
                                   CONFIG_GPU_RESOURCE_CACHE_CAPACITY_DEFAULT);
    CONFIG_CHECK(CheckGpuResourceConfigCacheCapacity(str));
    value = std::stoll(str);
    return Status::OK();
}

Status
Config::GetGpuResourceConfigCacheThreshold(float& value) {
    bool gpu_resource_enable = false;
    CONFIG_CHECK(GetGpuResourceConfigEnable(gpu_resource_enable));
    if (!gpu_resource_enable) {
        std::string msg = "GPU not supported. Possible reason: gpu_resource_config.enable is set to false.";
        return Status(SERVER_UNSUPPORTED_ERROR, msg);
    }
    std::string str = GetConfigStr(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_CACHE_THRESHOLD,
                                   CONFIG_GPU_RESOURCE_CACHE_THRESHOLD_DEFAULT);
    CONFIG_CHECK(CheckGpuResourceConfigCacheThreshold(str));
    value = std::stof(str);
    return Status::OK();
}

Status
Config::GetGpuResourceConfigSearchResources(std::vector<int64_t>& value) {
    bool gpu_resource_enable = false;
    CONFIG_CHECK(GetGpuResourceConfigEnable(gpu_resource_enable));
    if (!gpu_resource_enable) {
        std::string msg = "GPU not supported. Possible reason: gpu_resource_config.enable is set to false.";
        return Status(SERVER_UNSUPPORTED_ERROR, msg);
    }
    std::string str = GetConfigSequenceStr(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_SEARCH_RESOURCES,
                                           CONFIG_GPU_RESOURCE_DELIMITER, CONFIG_GPU_RESOURCE_SEARCH_RESOURCES_DEFAULT);
    std::vector<std::string> res_vec;
    server::StringHelpFunctions::SplitStringByDelimeter(str, CONFIG_GPU_RESOURCE_DELIMITER, res_vec);
    CONFIG_CHECK(CheckGpuResourceConfigSearchResources(res_vec));
    for (std::string& res : res_vec) {
        value.push_back(std::stoll(res.substr(3)));
    }
    return Status::OK();
}

Status
Config::GetGpuResourceConfigBuildIndexResources(std::vector<int64_t>& value) {
    bool gpu_resource_enable = false;
    CONFIG_CHECK(GetGpuResourceConfigEnable(gpu_resource_enable));
    if (!gpu_resource_enable) {
        std::string msg = "GPU not supported. Possible reason: gpu_resource_config.enable is set to false.";
        return Status(SERVER_UNSUPPORTED_ERROR, msg);
    }
    std::string str =
        GetConfigSequenceStr(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES,
                             CONFIG_GPU_RESOURCE_DELIMITER, CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES_DEFAULT);
    std::vector<std::string> res_vec;
    server::StringHelpFunctions::SplitStringByDelimeter(str, CONFIG_GPU_RESOURCE_DELIMITER, res_vec);
    CONFIG_CHECK(CheckGpuResourceConfigBuildIndexResources(res_vec));
    for (std::string& res : res_vec) {
        value.push_back(std::stoll(res.substr(3)));
    }
    return Status::OK();
}
#endif

/* tracing config */
Status
Config::GetTracingConfigJsonConfigPath(std::string& value) {
    value = GetConfigStr(CONFIG_TRACING, CONFIG_TRACING_JSON_CONFIG_PATH, "");
    if (!value.empty()) {
        std::ifstream tracer_config(value);
        Status s = tracer_config.good() ? Status::OK()
                                        : Status(SERVER_INVALID_ARGUMENT, "Failed to open tracer config file " + value +
                                                                              ": " + std::strerror(errno));
        tracer_config.close();
        return s;
    }
    return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
/* server config */
Status
Config::SetServerConfigAddress(const std::string& value) {
    CONFIG_CHECK(CheckServerConfigAddress(value));
    return SetConfigValueInMem(CONFIG_SERVER, CONFIG_SERVER_ADDRESS, value);
}

Status
Config::SetServerConfigPort(const std::string& value) {
    CONFIG_CHECK(CheckServerConfigPort(value));
    return SetConfigValueInMem(CONFIG_SERVER, CONFIG_SERVER_PORT, value);
}

Status
Config::SetServerConfigDeployMode(const std::string& value) {
    CONFIG_CHECK(CheckServerConfigDeployMode(value));
    return SetConfigValueInMem(CONFIG_SERVER, CONFIG_SERVER_DEPLOY_MODE, value);
}

Status
Config::SetServerConfigTimeZone(const std::string& value) {
    CONFIG_CHECK(CheckServerConfigTimeZone(value));
    return SetConfigValueInMem(CONFIG_SERVER, CONFIG_SERVER_TIME_ZONE, value);
}

/* db config */
Status
Config::SetDBConfigBackendUrl(const std::string& value) {
    CONFIG_CHECK(CheckDBConfigBackendUrl(value));
    return SetConfigValueInMem(CONFIG_DB, CONFIG_DB_BACKEND_URL, value);
}

Status
Config::SetDBConfigArchiveDiskThreshold(const std::string& value) {
    CONFIG_CHECK(CheckDBConfigArchiveDiskThreshold(value));
    return SetConfigValueInMem(CONFIG_DB, CONFIG_DB_ARCHIVE_DISK_THRESHOLD, value);
}

Status
Config::SetDBConfigArchiveDaysThreshold(const std::string& value) {
    CONFIG_CHECK(CheckDBConfigArchiveDaysThreshold(value));
    return SetConfigValueInMem(CONFIG_DB, CONFIG_DB_ARCHIVE_DAYS_THRESHOLD, value);
}

Status
Config::SetDBConfigInsertBufferSize(const std::string& value) {
    CONFIG_CHECK(CheckDBConfigInsertBufferSize(value));
    return SetConfigValueInMem(CONFIG_DB, CONFIG_DB_INSERT_BUFFER_SIZE, value);
}

/* storage config */
Status
Config::SetStorageConfigPrimaryPath(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigPrimaryPath(value));
    return SetConfigValueInMem(CONFIG_DB, CONFIG_STORAGE_PRIMARY_PATH, value);
}

Status
Config::SetStorageConfigSecondaryPath(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigSecondaryPath(value));
    return SetConfigValueInMem(CONFIG_DB, CONFIG_STORAGE_SECONDARY_PATH, value);
}

Status
Config::SetStorageConfigMinioEnable(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigMinioEnable(value));
    return SetConfigValueInMem(CONFIG_METRIC, CONFIG_METRIC_ENABLE_MONITOR, value);
}

Status
Config::SetStorageConfigMinioAddress(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigMinioAddress(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_ADDRESS, value);
}

Status
Config::SetStorageConfigMinioPort(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigMinioPort(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_PORT, value);
}

Status
Config::SetStorageConfigMinioAccessKey(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigMinioAccessKey(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_ACCESS_KEY, value);
}

Status
Config::SetStorageConfigMinioSecretKey(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigMinioSecretKey(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_SECRET_KEY, value);
}

Status
Config::SetStorageConfigMinioBucket(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigMinioBucket(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_MINIO_BUCKET, value);
}

/* metric config */
Status
Config::SetMetricConfigEnableMonitor(const std::string& value) {
    CONFIG_CHECK(CheckMetricConfigEnableMonitor(value));
    return SetConfigValueInMem(CONFIG_METRIC, CONFIG_METRIC_ENABLE_MONITOR, value);
}

Status
Config::SetMetricConfigCollector(const std::string& value) {
    CONFIG_CHECK(CheckMetricConfigCollector(value));
    return SetConfigValueInMem(CONFIG_METRIC, CONFIG_METRIC_COLLECTOR, value);
}

Status
Config::SetMetricConfigPrometheusPort(const std::string& value) {
    CONFIG_CHECK(CheckMetricConfigPrometheusPort(value));
    return SetConfigValueInMem(CONFIG_METRIC, CONFIG_METRIC_PROMETHEUS_PORT, value);
}

/* cache config */
Status
Config::SetCacheConfigCpuCacheCapacity(const std::string& value) {
    CONFIG_CHECK(CheckCacheConfigCpuCacheCapacity(value));
    return SetConfigValueInMem(CONFIG_CACHE, CONFIG_CACHE_CPU_CACHE_CAPACITY, value);
}

Status
Config::SetCacheConfigCpuCacheThreshold(const std::string& value) {
    CONFIG_CHECK(CheckCacheConfigCpuCacheThreshold(value));
    return SetConfigValueInMem(CONFIG_CACHE, CONFIG_CACHE_CPU_CACHE_THRESHOLD, value);
}

Status
Config::SetCacheConfigCacheInsertData(const std::string& value) {
    CONFIG_CHECK(CheckCacheConfigCacheInsertData(value));
    return SetConfigValueInMem(CONFIG_CACHE, CONFIG_CACHE_CACHE_INSERT_DATA, value);
}

/* engine config */
Status
Config::SetEngineConfigUseBlasThreshold(const std::string& value) {
    CONFIG_CHECK(CheckEngineConfigUseBlasThreshold(value));
    return SetConfigValueInMem(CONFIG_ENGINE, CONFIG_ENGINE_USE_BLAS_THRESHOLD, value);
}

Status
Config::SetEngineConfigOmpThreadNum(const std::string& value) {
    CONFIG_CHECK(CheckEngineConfigOmpThreadNum(value));
    return SetConfigValueInMem(CONFIG_ENGINE, CONFIG_ENGINE_OMP_THREAD_NUM, value);
}

#ifdef MILVUS_GPU_VERSION
Status
Config::SetEngineConfigGpuSearchThreshold(const std::string& value) {
    CONFIG_CHECK(CheckEngineConfigGpuSearchThreshold(value));
    return SetConfigValueInMem(CONFIG_ENGINE, CONFIG_ENGINE_GPU_SEARCH_THRESHOLD, value);
}
#endif

/* gpu resource config */
#ifdef MILVUS_GPU_VERSION
Status
Config::SetGpuResourceConfigEnable(const std::string& value) {
    CONFIG_CHECK(CheckGpuResourceConfigEnable(value));
    return SetConfigValueInMem(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_ENABLE, value);
}

Status
Config::SetGpuResourceConfigCacheCapacity(const std::string& value) {
    CONFIG_CHECK(CheckGpuResourceConfigCacheCapacity(value));
    return SetConfigValueInMem(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_CACHE_CAPACITY, value);
}

Status
Config::SetGpuResourceConfigCacheThreshold(const std::string& value) {
    CONFIG_CHECK(CheckGpuResourceConfigCacheThreshold(value));
    return SetConfigValueInMem(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_CACHE_THRESHOLD, value);
}

Status
Config::SetGpuResourceConfigSearchResources(const std::string& value) {
    std::vector<std::string> res_vec;
    server::StringHelpFunctions::SplitStringByDelimeter(value, CONFIG_GPU_RESOURCE_DELIMITER, res_vec);
    CONFIG_CHECK(CheckGpuResourceConfigSearchResources(res_vec));
    return SetConfigValueInMem(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, value);
}

Status
Config::SetGpuResourceConfigBuildIndexResources(const std::string& value) {
    std::vector<std::string> res_vec;
    server::StringHelpFunctions::SplitStringByDelimeter(value, CONFIG_GPU_RESOURCE_DELIMITER, res_vec);
    CONFIG_CHECK(CheckGpuResourceConfigBuildIndexResources(res_vec));
    return SetConfigValueInMem(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, value);
}
#endif

}  // namespace server
}  // namespace milvus
