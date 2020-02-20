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

#include <sys/stat.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/YamlConfigMgr.h"
#include "server/Config.h"
#include "thirdparty/nlohmann/json.hpp"
#include "utils/CommonUtil.h"
#include "utils/StringHelpFunctions.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>

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

    ConfigMgr* mgr = YamlConfigMgr::GetInstance();
    Status s = mgr->LoadConfigFile(filename);
    if (!s.ok()) {
        return s;
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

    std::string server_web_port;
    CONFIG_CHECK(GetServerConfigWebPort(server_web_port));

    /* db config */
    std::string db_backend_url;
    CONFIG_CHECK(GetDBConfigBackendUrl(db_backend_url));

    int64_t db_archive_disk_threshold;
    CONFIG_CHECK(GetDBConfigArchiveDiskThreshold(db_archive_disk_threshold));

    int64_t db_archive_days_threshold;
    CONFIG_CHECK(GetDBConfigArchiveDaysThreshold(db_archive_days_threshold));

    /* storage config */
    std::string storage_primary_path;
    CONFIG_CHECK(GetStorageConfigPrimaryPath(storage_primary_path));

    std::string storage_secondary_path;
    CONFIG_CHECK(GetStorageConfigSecondaryPath(storage_secondary_path));

    bool storage_s3_enable;
    CONFIG_CHECK(GetStorageConfigS3Enable(storage_s3_enable));
    std::cout << "S3 " << (storage_s3_enable ? "ENABLED !" : "DISABLED !") << std::endl;

    std::string storage_s3_address;
    CONFIG_CHECK(GetStorageConfigS3Address(storage_s3_address));

    std::string storage_s3_port;
    CONFIG_CHECK(GetStorageConfigS3Port(storage_s3_port));

    std::string storage_s3_access_key;
    CONFIG_CHECK(GetStorageConfigS3AccessKey(storage_s3_access_key));

    std::string storage_s3_secret_key;
    CONFIG_CHECK(GetStorageConfigS3SecretKey(storage_s3_secret_key));

    std::string storage_s3_bucket;
    CONFIG_CHECK(GetStorageConfigS3Bucket(storage_s3_bucket));

    /* metric config */
    bool metric_enable_monitor;
    CONFIG_CHECK(GetMetricConfigEnableMonitor(metric_enable_monitor));

    std::string metric_address;
    CONFIG_CHECK(GetMetricConfigAddress(metric_address));

    std::string metric_port;
    CONFIG_CHECK(GetMetricConfigPort(metric_port));

    /* cache config */
    int64_t cache_cpu_cache_capacity;
    CONFIG_CHECK(GetCacheConfigCpuCacheCapacity(cache_cpu_cache_capacity));

    float cache_cpu_cache_threshold;
    CONFIG_CHECK(GetCacheConfigCpuCacheThreshold(cache_cpu_cache_threshold));

    int64_t cache_insert_buffer_size;
    CONFIG_CHECK(GetCacheConfigInsertBufferSize(cache_insert_buffer_size));

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
    CONFIG_CHECK(SetServerConfigWebPort(CONFIG_SERVER_WEB_PORT_DEFAULT));

    /* db config */
    CONFIG_CHECK(SetDBConfigBackendUrl(CONFIG_DB_BACKEND_URL_DEFAULT));
    CONFIG_CHECK(SetDBConfigArchiveDiskThreshold(CONFIG_DB_ARCHIVE_DISK_THRESHOLD_DEFAULT));
    CONFIG_CHECK(SetDBConfigArchiveDaysThreshold(CONFIG_DB_ARCHIVE_DAYS_THRESHOLD_DEFAULT));

    /* storage config */
    CONFIG_CHECK(SetStorageConfigPrimaryPath(CONFIG_STORAGE_PRIMARY_PATH_DEFAULT));
    CONFIG_CHECK(SetStorageConfigSecondaryPath(CONFIG_STORAGE_SECONDARY_PATH_DEFAULT));
    CONFIG_CHECK(SetStorageConfigS3Enable(CONFIG_STORAGE_S3_ENABLE_DEFAULT));
    CONFIG_CHECK(SetStorageConfigS3Address(CONFIG_STORAGE_S3_ADDRESS_DEFAULT));
    CONFIG_CHECK(SetStorageConfigS3Port(CONFIG_STORAGE_S3_PORT_DEFAULT));
    CONFIG_CHECK(SetStorageConfigS3AccessKey(CONFIG_STORAGE_S3_ACCESS_KEY_DEFAULT));
    CONFIG_CHECK(SetStorageConfigS3SecretKey(CONFIG_STORAGE_S3_SECRET_KEY_DEFAULT));
    CONFIG_CHECK(SetStorageConfigS3Bucket(CONFIG_STORAGE_S3_BUCKET_DEFAULT));

    /* metric config */
    CONFIG_CHECK(SetMetricConfigEnableMonitor(CONFIG_METRIC_ENABLE_MONITOR_DEFAULT));
    CONFIG_CHECK(SetMetricConfigAddress(CONFIG_METRIC_ADDRESS_DEFAULT));
    CONFIG_CHECK(SetMetricConfigPort(CONFIG_METRIC_PORT_DEFAULT));

    /* cache config */
    CONFIG_CHECK(SetCacheConfigCpuCacheCapacity(CONFIG_CACHE_CPU_CACHE_CAPACITY_DEFAULT));
    CONFIG_CHECK(SetCacheConfigCpuCacheThreshold(CONFIG_CACHE_CPU_CACHE_THRESHOLD_DEFAULT));
    CONFIG_CHECK(SetCacheConfigInsertBufferSize(CONFIG_CACHE_INSERT_BUFFER_SIZE_DEFAULT));
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
        } else if (child_key == CONFIG_CACHE_INSERT_BUFFER_SIZE) {
            return SetCacheConfigInsertBufferSize(value);
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
    bool exist_error = milvus_config_version_map.at(MILVUS_VERSION) != value;
    fiu_do_on("check_config_version_fail", exist_error = true);
    if (exist_error) {
        std::string msg = "Invalid config version: " + value +
                          ". Expected config version: " + milvus_config_version_map.at(MILVUS_VERSION);
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

/* server config */
Status
Config::CheckServerConfigAddress(const std::string& value) {
    auto exist_error = !ValidationUtil::ValidateIpAddress(value).ok();
    fiu_do_on("check_config_address_fail", exist_error = true);

    if (exist_error) {
        std::string msg =
            "Invalid server IP address: " + value + ". Possible reason: server_config.address is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckServerConfigPort(const std::string& value) {
    auto exist_error = !ValidationUtil::ValidateStringIsNumber(value).ok();
    fiu_do_on("check_config_port_fail", exist_error = true);

    if (exist_error) {
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
    fiu_return_on("check_config_deploy_mode_fail",
                  Status(SERVER_INVALID_ARGUMENT,
                         "server_config.deploy_mode is not one of single, cluster_readonly, and cluster_writable."));

    if (value != "single" && value != "cluster_readonly" && value != "cluster_writable") {
        return Status(SERVER_INVALID_ARGUMENT,
                      "server_config.deploy_mode is not one of single, cluster_readonly, and cluster_writable.");
    }
    return Status::OK();
}

Status
Config::CheckServerConfigTimeZone(const std::string& value) {
    fiu_return_on("check_config_time_zone_fail",
                  Status(SERVER_INVALID_ARGUMENT, "Invalid server_config.time_zone: " + value));

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

Status
Config::CheckServerConfigWebPort(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg =
            "Invalid web server port: " + value + ". Possible reason: server_config.web_port is not a number.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int32_t port = std::stoi(value);
        if (!(port > 1024 && port < 65535)) {
            std::string msg = "Invalid web server port: " + value +
                              ". Possible reason: server_config.web_port is not in range [1025, 65534].";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

/* DB config */
Status
Config::CheckDBConfigBackendUrl(const std::string& value) {
    auto exist_error = !ValidationUtil::ValidateDbURI(value).ok();
    fiu_do_on("check_config_backend_url_fail", exist_error = true);

    if (exist_error) {
        std::string msg =
            "Invalid backend url: " + value + ". Possible reason: db_config.db_backend_url is invalid. " +
            "The correct format should be like sqlite://:@:/ or mysql://root:123456@127.0.0.1:3306/milvus.";
        return Status(SERVER_INVALID_ARGUMENT, "invalid db_backend_url: " + value);
    }
    return Status::OK();
}

Status
Config::CheckDBConfigArchiveDiskThreshold(const std::string& value) {
    auto exist_error = !ValidationUtil::ValidateStringIsNumber(value).ok();
    fiu_do_on("check_config_archive_disk_threshold_fail", exist_error = true);

    if (exist_error) {
        std::string msg = "Invalid archive disk threshold: " + value +
                          ". Possible reason: db_config.archive_disk_threshold is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckDBConfigArchiveDaysThreshold(const std::string& value) {
    auto exist_error = !ValidationUtil::ValidateStringIsNumber(value).ok();
    fiu_do_on("check_config_archive_days_threshold_fail", exist_error = true);

    if (exist_error) {
        std::string msg = "Invalid archive days threshold: " + value +
                          ". Possible reason: db_config.archive_days_threshold is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

/* storage config */
Status
Config::CheckStorageConfigPrimaryPath(const std::string& value) {
    fiu_return_on("check_config_primary_path_fail", Status(SERVER_INVALID_ARGUMENT, ""));
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.db_path is empty.");
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigSecondaryPath(const std::string& value) {
    fiu_return_on("check_config_secondary_path_fail", Status(SERVER_INVALID_ARGUMENT, ""));
    return Status::OK();
}

Status
Config::CheckStorageConfigS3Enable(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsBool(value).ok()) {
        std::string msg =
            "Invalid storage config: " + value + ". Possible reason: storage_config.s3_enable is not a boolean.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigS3Address(const std::string& value) {
    if (!ValidationUtil::ValidateIpAddress(value).ok()) {
        std::string msg = "Invalid s3 address: " + value + ". Possible reason: storage_config.s3_address is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigS3Port(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid s3 port: " + value + ". Possible reason: storage_config.s3_port is not a number.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int32_t port = std::stoi(value);
        if (!(port > 1024 && port < 65535)) {
            std::string msg = "Invalid s3 port: " + value +
                              ". Possible reason: storage_config.s3_port is not in range (1024, 65535).";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigS3AccessKey(const std::string& value) {
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.s3_access_key is empty.");
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigS3SecretKey(const std::string& value) {
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.s3_secret_key is empty.");
    }
    return Status::OK();
}

Status
Config::CheckStorageConfigS3Bucket(const std::string& value) {
    if (value.empty()) {
        return Status(SERVER_INVALID_ARGUMENT, "storage_config.s3_bucket is empty.");
    }
    return Status::OK();
}

/* metric config */
Status
Config::CheckMetricConfigEnableMonitor(const std::string& value) {
    auto exist_error = !ValidationUtil::ValidateStringIsBool(value).ok();
    fiu_do_on("check_config_enable_monitor_fail", exist_error = true);

    if (exist_error) {
        std::string msg =
            "Invalid metric config: " + value + ". Possible reason: metric_config.enable_monitor is not a boolean.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckMetricConfigAddress(const std::string& value) {
    if (!ValidationUtil::ValidateIpAddress(value).ok()) {
        std::string msg = "Invalid metric ip: " + value + ". Possible reason: metric_config.ip is invalid.";
        return Status(SERVER_INVALID_ARGUMENT, "Invalid metric config ip: " + value);
    }
    return Status::OK();
}

Status
Config::CheckMetricConfigPort(const std::string& value) {
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid metric port: " + value + ". Possible reason: metric_config.port is not a number.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int32_t port = std::stoi(value);
        if (!(port > 1024 && port < 65535)) {
            std::string msg = "Invalid metric port: " + value +
                              ". Possible reason: metric_config.port is not in range (1024, 65535).";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

/* cache config */
Status
Config::CheckCacheConfigCpuCacheCapacity(const std::string& value) {
    fiu_return_on("check_config_cpu_cache_capacity_fail", Status(SERVER_INVALID_ARGUMENT, ""));

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
        CONFIG_CHECK(GetCacheConfigInsertBufferSize(buffer_value));

        int64_t insert_buffer_size = buffer_value * GB;
        fiu_do_on("Config.CheckCacheConfigCpuCacheCapacity.large_insert_buffer", insert_buffer_size = total_mem + 1);
        if (insert_buffer_size + cpu_cache_capacity >= total_mem) {
            std::string msg = "Invalid cpu cache capacity: " + value +
                              ". Possible reason: sum of cache_config.cpu_cache_capacity and "
                              "cache_config.insert_buffer_size exceeds system memory.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

Status
Config::CheckCacheConfigCpuCacheThreshold(const std::string& value) {
    fiu_return_on("check_config_cpu_cache_threshold_fail", Status(SERVER_INVALID_ARGUMENT, ""));

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
Config::CheckCacheConfigInsertBufferSize(const std::string& value) {
    fiu_return_on("check_config_insert_buffer_size_fail", Status(SERVER_INVALID_ARGUMENT, ""));
    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid insert buffer size: " + value +
                          ". Possible reason: cache_config.insert_buffer_size is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    } else {
        int64_t buffer_size = std::stoll(value) * GB;
        if (buffer_size <= 0) {
            std::string msg = "Invalid insert buffer size: " + value +
                              ". Possible reason: cache_config.insert_buffer_size is not a positive integer.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }

        uint64_t total_mem = 0, free_mem = 0;
        CommonUtil::GetSystemMemInfo(total_mem, free_mem);
        if (buffer_size >= total_mem) {
            std::string msg = "Invalid insert buffer size: " + value +
                              ". Possible reason: cache_config.insert_buffer_size exceeds system memory.";
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    }
    return Status::OK();
}

Status
Config::CheckCacheConfigCacheInsertData(const std::string& value) {
    fiu_return_on("check_config_cache_insert_data_fail", Status(SERVER_INVALID_ARGUMENT, ""));

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
    fiu_return_on("check_config_use_blas_threshold_fail", Status(SERVER_INVALID_ARGUMENT, ""));

    if (!ValidationUtil::ValidateStringIsNumber(value).ok()) {
        std::string msg = "Invalid use blas threshold: " + value +
                          ". Possible reason: engine_config.use_blas_threshold is not a positive integer.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckEngineConfigOmpThreadNum(const std::string& value) {
    fiu_return_on("check_config_omp_thread_num_fail", Status(SERVER_INVALID_ARGUMENT, ""));

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
    fiu_return_on("check_config_gpu_search_threshold_fail", Status(SERVER_INVALID_ARGUMENT, ""));

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
    fiu_return_on("check_config_gpu_resource_enable_fail", Status(SERVER_INVALID_ARGUMENT, ""));

    if (!ValidationUtil::ValidateStringIsBool(value).ok()) {
        std::string msg =
            "Invalid gpu resource config: " + value + ". Possible reason: gpu_resource_config.enable is not a boolean.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
    return Status::OK();
}

Status
Config::CheckGpuResourceConfigCacheCapacity(const std::string& value) {
    fiu_return_on("check_gpu_resource_config_cache_capacity_fail", Status(SERVER_INVALID_ARGUMENT, ""));

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
    fiu_return_on("check_config_gpu_resource_cache_threshold_fail", Status(SERVER_INVALID_ARGUMENT, ""));

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
    fiu_return_on("check_gpu_resource_config_search_fail", Status(SERVER_INVALID_ARGUMENT, ""));

    if (value.empty()) {
        std::string msg =
            "Invalid gpu search resource. "
            "Possible reason: gpu_resource_config.search_resources is empty.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    std::unordered_set<std::string> value_set;
    for (auto& resource : value) {
        CONFIG_CHECK(CheckGpuResource(resource));
        value_set.insert(resource);
    }

    if (value_set.size() != value.size()) {
        std::string msg =
            "Invalid gpu build search resource. "
            "Possible reason: gpu_resource_config.gpu_search_resources contains duplicate resources.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    return Status::OK();
}

Status
Config::CheckGpuResourceConfigBuildIndexResources(const std::vector<std::string>& value) {
    fiu_return_on("check_gpu_resource_config_build_index_fail", Status(SERVER_INVALID_ARGUMENT, ""));

    if (value.empty()) {
        std::string msg =
            "Invalid gpu build index resource. "
            "Possible reason: gpu_resource_config.build_index_resources is empty.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    std::unordered_set<std::string> value_set;
    for (auto& resource : value) {
        CONFIG_CHECK(CheckGpuResource(resource));
        value_set.insert(resource);
    }

    if (value_set.size() != value.size()) {
        std::string msg =
            "Invalid gpu build index resource. "
            "Possible reason: gpu_resource_config.build_index_resources contains duplicate resources.";
        return Status(SERVER_INVALID_ARGUMENT, msg);
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
    return config_map_[parent_key].count(child_key) != 0;
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

Status
Config::GetServerConfigWebPort(std::string& value) {
    value = GetConfigStr(CONFIG_SERVER, CONFIG_SERVER_WEB_PORT, CONFIG_SERVER_WEB_PORT_DEFAULT);
    return CheckServerConfigWebPort(value);
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
Config::GetDBConfigPreloadTable(std::string& value) {
    value = GetConfigStr(CONFIG_DB, CONFIG_DB_PRELOAD_TABLE);
    return Status::OK();
}

/* storage config */
Status
Config::GetStorageConfigPrimaryPath(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_PRIMARY_PATH, CONFIG_STORAGE_PRIMARY_PATH_DEFAULT);
    return CheckStorageConfigPrimaryPath(value);
}

Status
Config::GetStorageConfigSecondaryPath(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_SECONDARY_PATH, CONFIG_STORAGE_SECONDARY_PATH_DEFAULT);
    return CheckStorageConfigSecondaryPath(value);
}

Status
Config::GetStorageConfigS3Enable(bool& value) {
    std::string str = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_S3_ENABLE, CONFIG_STORAGE_S3_ENABLE_DEFAULT);
    CONFIG_CHECK(CheckStorageConfigS3Enable(str));
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    value = (str == "true" || str == "on" || str == "yes" || str == "1");
    return Status::OK();
}

Status
Config::GetStorageConfigS3Address(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_S3_ADDRESS, CONFIG_STORAGE_S3_ADDRESS_DEFAULT);
    return CheckStorageConfigS3Address(value);
}

Status
Config::GetStorageConfigS3Port(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_S3_PORT, CONFIG_STORAGE_S3_PORT_DEFAULT);
    return CheckStorageConfigS3Port(value);
}

Status
Config::GetStorageConfigS3AccessKey(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_S3_ACCESS_KEY, CONFIG_STORAGE_S3_ACCESS_KEY_DEFAULT);
    return Status::OK();
}

Status
Config::GetStorageConfigS3SecretKey(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_S3_SECRET_KEY, CONFIG_STORAGE_S3_SECRET_KEY_DEFAULT);
    return Status::OK();
}

Status
Config::GetStorageConfigS3Bucket(std::string& value) {
    value = GetConfigStr(CONFIG_STORAGE, CONFIG_STORAGE_S3_BUCKET, CONFIG_STORAGE_S3_BUCKET_DEFAULT);
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
Config::GetMetricConfigAddress(std::string& value) {
    value = GetConfigStr(CONFIG_METRIC, CONFIG_METRIC_ADDRESS, CONFIG_METRIC_ADDRESS_DEFAULT);
    return Status::OK();
}

Status
Config::GetMetricConfigPort(std::string& value) {
    value = GetConfigStr(CONFIG_METRIC, CONFIG_METRIC_PORT, CONFIG_METRIC_PORT_DEFAULT);
    return CheckMetricConfigPort(value);
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
Config::GetCacheConfigInsertBufferSize(int64_t& value) {
    std::string str =
        GetConfigStr(CONFIG_CACHE, CONFIG_CACHE_INSERT_BUFFER_SIZE, CONFIG_CACHE_INSERT_BUFFER_SIZE_DEFAULT);
    CONFIG_CHECK(CheckCacheConfigInsertBufferSize(str));
    value = std::stoll(str);
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
    fiu_do_on("Config.GetGpuResourceConfigCacheCapacity.diable_gpu_resource", gpu_resource_enable = false);
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
    fiu_do_on("Config.GetGpuResourceConfigCacheThreshold.diable_gpu_resource", gpu_resource_enable = false);
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
    fiu_do_on("get_gpu_config_search_resources.disable_gpu_resource_fail", gpu_resource_enable = false);
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
    fiu_do_on("get_gpu_config_build_index_resources.disable_gpu_resource_fail", gpu_resource_enable = false);
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
    fiu_do_on("get_config_json_config_path_fail", value = "error_config_json_path");
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

Status
Config::SetServerConfigWebPort(const std::string& value) {
    CONFIG_CHECK(CheckServerConfigWebPort(value));
    return SetConfigValueInMem(CONFIG_SERVER, CONFIG_SERVER_WEB_PORT, value);
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

/* storage config */
Status
Config::SetStorageConfigPrimaryPath(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigPrimaryPath(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_PRIMARY_PATH, value);
}

Status
Config::SetStorageConfigSecondaryPath(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigSecondaryPath(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_SECONDARY_PATH, value);
}

Status
Config::SetStorageConfigS3Enable(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigS3Enable(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_S3_ENABLE, value);
}

Status
Config::SetStorageConfigS3Address(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigS3Address(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_S3_ADDRESS, value);
}

Status
Config::SetStorageConfigS3Port(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigS3Port(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_S3_PORT, value);
}

Status
Config::SetStorageConfigS3AccessKey(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigS3AccessKey(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_S3_ACCESS_KEY, value);
}

Status
Config::SetStorageConfigS3SecretKey(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigS3SecretKey(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_S3_SECRET_KEY, value);
}

Status
Config::SetStorageConfigS3Bucket(const std::string& value) {
    CONFIG_CHECK(CheckStorageConfigS3Bucket(value));
    return SetConfigValueInMem(CONFIG_STORAGE, CONFIG_STORAGE_S3_BUCKET, value);
}

/* metric config */
Status
Config::SetMetricConfigEnableMonitor(const std::string& value) {
    CONFIG_CHECK(CheckMetricConfigEnableMonitor(value));
    return SetConfigValueInMem(CONFIG_METRIC, CONFIG_METRIC_ENABLE_MONITOR, value);
}

Status
Config::SetMetricConfigAddress(const std::string& value) {
    CONFIG_CHECK(CheckMetricConfigAddress(value));
    return SetConfigValueInMem(CONFIG_METRIC, CONFIG_METRIC_ADDRESS, value);
}

Status
Config::SetMetricConfigPort(const std::string& value) {
    CONFIG_CHECK(CheckMetricConfigPort(value));
    return SetConfigValueInMem(CONFIG_METRIC, CONFIG_METRIC_PORT, value);
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
Config::SetCacheConfigInsertBufferSize(const std::string& value) {
    CONFIG_CHECK(CheckCacheConfigInsertBufferSize(value));
    return SetConfigValueInMem(CONFIG_CACHE, CONFIG_CACHE_INSERT_BUFFER_SIZE, value);
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
/* gpu resource config */
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
