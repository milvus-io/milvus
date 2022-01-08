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

#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/ConfigNode.h"
#include "utils/Status.h"

namespace milvus {
namespace server {

using ConfigCallBackF = std::function<Status(const std::string&)>;

extern const char* CONFIG_NODE_DELIMITER;
extern const char* CONFIG_VERSION;

/* cluster config */
extern const char* CONFIG_CLUSTER;
extern const char* CONFIG_CLUSTER_ENABLE;
extern const char* CONFIG_CLUSTER_ENABLE_DEFAULT;
extern const char* CONFIG_CLUSTER_ROLE;
extern const char* CONFIG_CLUSTER_ROLE_DEFAULT;

/* general config */
extern const char* CONFIG_GENERAL;
extern const char* CONFIG_GENERAL_TIMEZONE;
extern const char* CONFIG_GENERAL_TIMEZONE_DEFAULT;
extern const char* CONFIG_GENERAL_METAURI;
extern const char* CONFIG_GENERAL_METAURI_DEFAULT;
extern const char* CONFIG_GENERAL_META_SSL_CA;
extern const char* CONFIG_GENERAL_META_SSL_KEY;
extern const char* CONFIG_GENERAL_META_SSL_CERT;
extern const char* CONFIG_GENERAL_SEARCH_RAW_ENABLE;
extern const char* CONFIG_GENERAL_SEARCH_ROW_ENABLE_DEFAULT;

/* network config */
extern const char* CONFIG_NETWORK;
extern const char* CONFIG_NETWORK_BIND_ADDRESS;
extern const char* CONFIG_NETWORK_BIND_ADDRESS_DEFAULT;
extern const char* CONFIG_NETWORK_BIND_PORT;
extern const char* CONFIG_NETWORK_BIND_PORT_DEFAULT;
extern const char* CONFIG_NETWORK_HTTP_ENABLE;
extern const char* CONFIG_NETWORK_HTTP_ENABLE_DEFAULT;
extern const char* CONFIG_NETWORK_HTTP_PORT;
extern const char* CONFIG_NETWORK_HTTP_PORT_DEFAULT;

/* db config */
extern const char* CONFIG_DB;
// extern const char* CONFIG_DB_BACKEND_URL;
// extern const char* CONFIG_DB_BACKEND_URL_DEFAULT;
extern const char* CONFIG_DB_ARCHIVE_DISK_THRESHOLD;
extern const char* CONFIG_DB_ARCHIVE_DISK_THRESHOLD_DEFAULT;
extern const char* CONFIG_DB_ARCHIVE_DAYS_THRESHOLD;
extern const char* CONFIG_DB_ARCHIVE_DAYS_THRESHOLD_DEFAULT;

/* storage config */
extern const char* CONFIG_STORAGE;
extern const char* CONFIG_STORAGE_PATH;
extern const char* CONFIG_STORAGE_PATH_DEFAULT;
extern const char* CONFIG_STORAGE_AUTO_FLUSH_INTERVAL;
extern const char* CONFIG_STORAGE_AUTO_FLUSH_INTERVAL_DEFAULT;
extern const char* CONFIG_STORAGE_FILE_CLEANUP_TIMEOUT;
extern const int64_t CONFIG_STORAGE_FILE_CLEANUP_TIMEOUT_MIN;
extern const int64_t CONFIG_STORAGE_FILE_CLEANUP_TIMEOUT_MAX;

/* cache config */
extern const char* CONFIG_CACHE;
extern const char* CONFIG_CACHE_CPU_CACHE_CAPACITY;
extern const char* CONFIG_CACHE_CPU_CACHE_CAPACITY_DEFAULT;
extern const char* CONFIG_CACHE_CPU_CACHE_THRESHOLD;
extern const char* CONFIG_CACHE_CPU_CACHE_THRESHOLD_DEFAULT;
extern const char* CONFIG_CACHE_INSERT_BUFFER_SIZE;
extern const char* CONFIG_CACHE_INSERT_BUFFER_SIZE_DEFAULT;
extern const char* CONFIG_CACHE_CACHE_INSERT_DATA;
extern const char* CONFIG_CACHE_CACHE_INSERT_DATA_DEFAULT;
extern const char* CONFIG_CACHE_PRELOAD_COLLECTION;
extern const char* CONFIG_CACHE_PRELOAD_COLLECTION_DEFAULT;

/* metric config */
extern const char* CONFIG_METRIC;
extern const char* CONFIG_METRIC_ENABLE_MONITOR;
extern const char* CONFIG_METRIC_ENABLE_MONITOR_DEFAULT;
extern const char* CONFIG_METRIC_ADDRESS;
extern const char* CONFIG_METRIC_ADDRESS_DEFAULT;
extern const char* CONFIG_METRIC_PORT;
extern const char* CONFIG_METRIC_PORT_DEFAULT;

/* engine config */
extern const char* CONFIG_ENGINE;
extern const char* CONFIG_ENGINE_USE_BLAS_THRESHOLD;
extern const char* CONFIG_ENGINE_USE_BLAS_THRESHOLD_DEFAULT;
extern const char* CONFIG_ENGINE_OMP_THREAD_NUM;
extern const char* CONFIG_ENGINE_OMP_THREAD_NUM_DEFAULT;
extern const char* CONFIG_ENGINE_SIMD_TYPE;
extern const char* CONFIG_ENGINE_SIMD_TYPE_DEFAULT;
extern const char* CONFIG_ENGINE_SEARCH_COMBINE_MAX_NQ;
extern const char* CONFIG_ENGINE_SEARCH_COMBINE_MAX_NQ_DEFAULT;
extern const char* CONFIG_ENGINE_MAX_PARTITION_NUM;
extern const char* CONFIG_ENGINE_MAX_PARTITION_NUM_DEFAULT;
/* fpga resource config*/
extern const char* CONFIG_FPGA_RESOURCE;
extern const char* CONFIG_FPGA_RESOURCE_ENABLE;
extern const char* CONFIG_FPGA_RESOURCE_CACHE_CAPACITY;
extern const char* CONFIG_FPGA_RESOURCE_CACHE_CAPACITY_DEFAULT; /* 1 GB */
extern const char* CONFIG_FPGA_RESOURCE_CACHE_THRESHOLD;
extern const char* CONFIG_FPGA_RESOURCE_CACHE_THRESHOLD_DEFAULT;
#ifdef MILVUS_FPGA_VERSION
extern const char* CONFIG_FPGA_RESOURCE_ENABLE_DEFAULT;
#else
extern const char* CONFIG_FPGA_RESOURCE_ENABLE_DEFAULT;
#endif
extern const char* CONFIG_FPGA_RESOURCE_DELIMITER;
extern const char* CONFIG_FPGA_RESOURCE_SEARCH_RESOURCES;
extern const char* CONFIG_FPGA_RESOURCE_SEARCH_RESOURCES_DEFAULT;
/* gpu resource config */
extern const char* CONFIG_GPU_RESOURCE;
extern const char* CONFIG_GPU_RESOURCE_ENABLE;
extern const char* CONFIG_GPU_RESOURCE_ENABLE_DEFAULT;
extern const char* CONFIG_GPU_RESOURCE_CACHE_CAPACITY;
extern const char* CONFIG_GPU_RESOURCE_CACHE_CAPACITY_DEFAULT;
extern const char* CONFIG_GPU_RESOURCE_CACHE_THRESHOLD;
extern const char* CONFIG_GPU_RESOURCE_CACHE_THRESHOLD_DEFAULT;
extern const char* CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD;
extern const char* CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD_DEFAULT;
extern const char* CONFIG_GPU_RESOURCE_DELIMITER;
extern const char* CONFIG_GPU_RESOURCE_SEARCH_RESOURCES;
extern const char* CONFIG_GPU_RESOURCE_SEARCH_RESOURCES_DEFAULT;
extern const char* CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES;
extern const char* CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES_DEFAULT;

/* tracing config */
extern const char* CONFIG_TRACING;
extern const char* CONFIG_TRACING_JSON_CONFIG_PATH;

/* wal config */
extern const char* CONFIG_WAL;
extern const char* CONFIG_WAL_ENABLE;
extern const char* CONFIG_WAL_ENABLE_DEFAULT;
extern const char* CONFIG_WAL_RECOVERY_ERROR_IGNORE;
extern const char* CONFIG_WAL_RECOVERY_ERROR_IGNORE_DEFAULT;
extern const char* CONFIG_WAL_BUFFER_SIZE;
extern const char* CONFIG_WAL_BUFFER_SIZE_DEFAULT;
extern const int64_t CONFIG_WAL_BUFFER_SIZE_MIN;
extern const int64_t CONFIG_WAL_BUFFER_SIZE_MAX;
extern const char* CONFIG_WAL_WAL_PATH;
extern const char* CONFIG_WAL_WAL_PATH_DEFAULT;

/* logs config */
extern const char* CONFIG_LOGS;
extern const char* CONFIG_LOGS_LEVEL;
extern const char* CONFIG_LOGS_LEVEL_DEFAULT;
extern const char* CONFIG_LOGS_TRACE_ENABLE;
extern const char* CONFIG_LOGS_TRACE_ENABLE_DEFAULT;
extern const char* CONFIG_LOGS_PATH;
extern const char* CONFIG_LOGS_MAX_LOG_FILE_SIZE;
extern const char* CONFIG_LOGS_MAX_LOG_FILE_SIZE_DEFAULT;
extern const int64_t CONFIG_LOGS_MAX_LOG_FILE_SIZE_MIN;
extern const int64_t CONFIG_LOGS_MAX_LOG_FILE_SIZE_MAX;
extern const char* CONFIG_LOGS_LOG_ROTATE_NUM;
extern const char* CONFIG_LOGS_LOG_ROTATE_NUM_DEFAULT;
extern const int64_t CONFIG_LOGS_LOG_ROTATE_NUM_MIN;
extern const int64_t CONFIG_LOGS_LOG_ROTATE_NUM_MAX;

class Config {
 private:
    Config();

 public:
    static Config&
    GetInstance();
    Status
    LoadConfigFile(const std::string& filename);
    Status
    ValidateConfig();
    Status
    ResetDefaultConfig();
    void
    GetConfigJsonStr(std::string& result, int64_t indent = -1);
    Status
    ProcessConfigCli(std::string& result, const std::string& cmd);

    Status
    GenUniqueIdentityID(const std::string& identity, std::string& uid);

    Status
    RegisterCallBack(const std::string& node, const std::string& sub_node, const std::string& key,
                     ConfigCallBackF& callback);

    Status
    CancelCallBack(const std::string& node, const std::string& sub_node, const std::string& key);

 private:
    ConfigNode&
    GetConfigRoot();
    ConfigNode&
    GetConfigNode(const std::string& name);
    bool
    ConfigNodeValid(const std::string& parent_key, const std::string& child_key);
    Status
    GetConfigValueInMem(const std::string& parent_key, const std::string& child_key, std::string& value);
    Status
    SetConfigValueInMem(const std::string& parent_key, const std::string& child_key, const std::string& value);
    Status
    GetConfigCli(std::string& value, const std::string& parent_key, const std::string& child_key);
    Status
    SetConfigCli(const std::string& parent_key, const std::string& child_key, const std::string& value);

    Status
    UpdateFileConfigFromMem(const std::string& parent_key, const std::string& child_key);

    ///////////////////////////////////////////////////////////////////////////
    Status
    CheckConfigVersion(const std::string& value);

    /* cluster config */
    Status
    CheckClusterConfigEnable(const std::string& value);
    Status
    CheckClusterConfigRole(const std::string& value);

    /* general config */
    Status
    CheckGeneralConfigTimezone(const std::string& value);
    Status
    CheckGeneralConfigMetaURI(const std::string& value);
    Status
    CheckGeneralConfigMetaSslCa(const std::string& value);
    Status
    CheckGeneralConfigMetaSslKey(const std::string& value);
    Status
    CheckGeneralConfigMetaSslCert(const std::string& value);

    /* network config */
    Status
    CheckNetworkConfigBindAddress(const std::string& value);
    Status
    CheckNetworkConfigBindPort(const std::string& value);
    Status
    CheckNetworkConfigHTTPEnable(const std::string& value);
    Status
    CheckNetworkConfigHTTPPort(const std::string& value);

    /* db config */
    // Status
    // CheckDBConfigBackendUrl(const std::string& value);
    Status
    CheckDBConfigArchiveDiskThreshold(const std::string& value);
    Status
    CheckDBConfigArchiveDaysThreshold(const std::string& value);

    /* storage config */
    Status
    CheckStorageConfigPath(const std::string& value);
    Status
    CheckStorageConfigAutoFlushInterval(const std::string& value);
    Status
    CheckStorageConfigFileCleanupTimeout(const std::string& value);

#ifdef MILVUS_WITH_AWS
    Status
    CheckStorageConfigS3Enable(const std::string& value);
    Status
    CheckStorageConfigS3Address(const std::string& value);
    Status
    CheckStorageConfigS3Port(const std::string& value);
    Status
    CheckStorageConfigS3AccessKey(const std::string& value);
    Status
    CheckStorageConfigS3SecretKey(const std::string& value);
    Status
    CheckStorageConfigS3Bucket(const std::string& value);
    Status
    CheckStorageConfigS3Region(const std::string& value);
#endif

    /* metric config */
    Status
    CheckMetricConfigEnableMonitor(const std::string& value);
    Status
    CheckMetricConfigAddress(const std::string& value);
    Status
    CheckMetricConfigPort(const std::string& value);

    /* cache config */
    Status
    CheckCacheConfigCpuCacheCapacity(const std::string& value);
    Status
    CheckCacheConfigCpuCacheThreshold(const std::string& value);
    Status
    CheckCacheConfigInsertBufferSize(const std::string& value);
    Status
    CheckCacheConfigCacheInsertData(const std::string& value);
    Status
    CheckCacheConfigPreloadCollection(const std::string& value);

    /* engine config */
    Status
    CheckEngineConfigUseBlasThreshold(const std::string& value);
    Status
    CheckEngineConfigOmpThreadNum(const std::string& value);
    Status
    CheckEngineConfigSimdType(const std::string& value);
    Status
    CheckEngineSearchCombineMaxNq(const std::string& value);
    Status
    CheckEngineConfigMaxPartitionNum(const std::string& value);
#ifdef MILVUS_FPGA_VERSION
    Status
    GetFpgaResourceConfigCacheThreshold(float& value);
    Status
    CheckFpgaResourceConfigEnable(const std::string& value);
    Status
    GetFpgaResourceConfigCacheCapacity(int64_t& value);
    Status
    CheckFpgaResourceConfigCacheThreshold(const std::string& value);
#endif
#ifdef MILVUS_GPU_VERSION
    /* gpu resource config */
    Status
    CheckGpuResourceConfigEnable(const std::string& value);
    Status
    CheckGpuResourceConfigCacheEnable(const std::string& value);
    Status
    CheckGpuResourceConfigCacheCapacity(const std::string& value);
    Status
    CheckGpuResourceConfigCacheThreshold(const std::string& value);
    Status
    CheckGpuResourceConfigGpuSearchThreshold(const std::string& value);
    Status
    CheckGpuResourceConfigSearchResources(const std::vector<std::string>& value);
    Status
    CheckGpuResourceConfigBuildIndexResources(const std::vector<std::string>& value);
#endif

    /* tracing config */
    Status
    CheckTracingConfigJsonConfigPath(const std::string& value);

    /* wal config */
    Status
    CheckWalConfigEnable(const std::string& value);
    Status
    CheckWalConfigRecoveryErrorIgnore(const std::string& value);
    Status
    CheckWalConfigBufferSize(const std::string& value);
    Status
    CheckWalConfigWalPath(const std::string& value);

    /* logs config */
    Status
    CheckLogsLevel(const std::string& level);
    Status
    CheckLogsTraceEnable(const std::string& value);
    Status
    CheckLogsPath(const std::string& value);
    Status
    CheckLogsMaxLogFileSize(const std::string& value);
    Status
    CheckLogsLogRotateNum(const std::string& value);
    Status
    CheckLogsLogToStdout(const std::string& value);
    Status
    CheckLogsLogToFile(const std::string& value);

    std::string
    GetConfigStr(const std::string& parent_key, const std::string& child_key, const std::string& default_value = "");
    std::string
    GetConfigSequenceStr(const std::string& parent_key, const std::string& child_key, const std::string& delim = ",",
                         const std::string& default_value = "");
    Status
    GetConfigVersion(std::string& value);

    Status
    ExecCallBacks(const std::string& node, const std::string& sub_node, const std::string& value);

 public:
    /* cluster config */
    Status
    GetClusterConfigEnable(bool& value);
    Status
    GetClusterConfigRole(std::string& value);

    /* general config */
    Status
    GetGeneralConfigTimezone(std::string& value);
    Status
    GetGeneralConfigMetaURI(std::string& value);
    Status
    GetGeneralConfigMetaSslCa(std::string& value);
    Status
    GetGeneralConfigMetaSslKey(std::string& value);
    Status
    GetGeneralConfigMetaSslCert(std::string& value);
    Status
    GetGeneralConfigSearchRawEnable(bool& value);

    /* network config */
    Status
    GetNetworkConfigBindAddress(std::string& value);
    Status
    GetNetworkConfigBindPort(std::string& value);
    Status
    GetNetworkConfigHTTPEnable(bool& value);
    Status
    GetNetworkConfigHTTPPort(std::string& value);

    /* db config */
    // Status
    // GetDBConfigBackendUrl(std::string& value);
    Status
    GetDBConfigArchiveDiskThreshold(int64_t& value);
    Status
    GetDBConfigArchiveDaysThreshold(int64_t& value);

    /* storage config */
    Status
    GetStorageConfigPath(std::string& value);
    Status
    GetStorageConfigAutoFlushInterval(int64_t& value);
    Status
    GetStorageConfigFileCleanupTimeup(int64_t& value);

#ifdef MILVUS_WITH_AWS
    Status
    GetStorageConfigS3Enable(bool& value);

    Status
    GetStorageConfigS3Address(std::string& value);

    Status
    GetStorageConfigS3Port(std::string& value);

    Status
    GetStorageConfigS3AccessKey(std::string& value);

    Status
    GetStorageConfigS3SecretKey(std::string& value);

    Status
    GetStorageConfigS3Bucket(std::string& value);

    Status
    GetStorageConfigS3Region(std::string& value);
#endif

    /* metric config */
    Status
    GetMetricConfigEnableMonitor(bool& value);
    Status
    GetMetricConfigAddress(std::string& value);
    Status
    GetMetricConfigPort(std::string& value);
    Status
    GetMetricConfigClusterLabel(std::string& value);
    Status
    GetMetricConfigInstanceLabel(std::string& value);

    /* cache config */
    Status
    GetCacheConfigCpuCacheCapacity(int64_t& value);
    Status
    GetCacheConfigCpuCacheThreshold(float& value);
    Status
    GetCacheConfigInsertBufferSize(int64_t& value);
    Status
    GetCacheConfigCacheInsertData(bool& value);
    Status
    GetCacheConfigPreloadCollection(std::string& value);

    /* engine config */
    Status
    GetEngineConfigUseBlasThreshold(int64_t& value);
    Status
    GetEngineConfigOmpThreadNum(int64_t& value);
    Status
    GetEngineConfigSimdType(std::string& value);
    Status
    GetEngineSearchCombineMaxNq(int64_t& value);
    Status
    GetEngineConfigMaxPartitionNum(int64_t& value);
#ifdef MILVUS_FPGA_VERSION

    Status
    GetFpgaResourceConfigEnable(bool& value);
    Status
    GetFpgaResourceConfigSearchResources(std::vector<int64_t>& value);
#endif
#ifdef MILVUS_GPU_VERSION
    /* gpu resource config */
    Status
    GetGpuResourceConfigEnable(bool& value);
    Status
    GetGpuResourceConfigCacheEnable(bool& value);
    Status
    GetGpuResourceConfigCacheCapacity(int64_t& value);
    Status
    GetGpuResourceConfigCacheThreshold(float& value);
    Status
    GetGpuResourceConfigGpuSearchThreshold(int64_t& value);
    Status
    GetGpuResourceConfigSearchResources(std::vector<int64_t>& value);
    Status
    GetGpuResourceConfigBuildIndexResources(std::vector<int64_t>& value);
#endif

    /* tracing config */
    Status
    GetTracingConfigJsonConfigPath(std::string& value);

    /* wal config */
    Status
    GetWalConfigEnable(bool& value);
    Status
    GetWalConfigRecoveryErrorIgnore(bool& value);
    Status
    GetWalConfigBufferSize(int64_t& value);
    Status
    GetWalConfigWalPath(std::string& value);

    /* logs config */
    Status
    GetLogsLevel(std::string& value);
    Status
    GetLogsTraceEnable(bool& value);
    Status
    GetLogsPath(std::string& value);
    Status
    GetLogsMaxLogFileSize(int64_t& value);
    Status
    GetLogsLogRotateNum(int64_t& value);
    Status
    GetLogsLogToStdout(bool& value);
    Status
    GetLogsLogToFile(bool& value);

    Status
    GetServerRestartRequired(bool& required);

 public:
    /* cluster config */
    Status
    SetClusterConfigEnable(const std::string& value);
    Status
    SetClusterConfigRole(const std::string& value);

    /* general config */
    Status
    SetGeneralConfigTimezone(const std::string& value);
    Status
    SetGeneralConfigMetaURI(const std::string& value);
    Status
    SetGeneralConfigMetaSslCa(const std::string& value);
    Status
    SetGeneralConfigMetaSslKey(const std::string& value);
    Status
    SetGeneralConfigMetaSslCert(const std::string& value);
    Status
    SetGeneralConfigSearchRawEnable(const std::string& value);

    /* network config */
    Status
    SetNetworkConfigBindAddress(const std::string& value);
    Status
    SetNetworkConfigBindPort(const std::string& value);
    Status
    SetNetworkConfigHTTPEnable(const std::string& value);
    Status
    SetNetworkConfigHTTPPort(const std::string& value);

    /* db config */
    // Status
    // SetDBConfigBackendUrl(const std::string& value);
    Status
    SetDBConfigArchiveDiskThreshold(const std::string& value);
    Status
    SetDBConfigArchiveDaysThreshold(const std::string& value);

    /* storage config */
    Status
    SetStorageConfigPath(const std::string& value);
    Status
    SetStorageConfigAutoFlushInterval(const std::string& value);
    Status
    SetStorageConfigFileCleanupTimeout(const std::string& value);

#ifdef MILVUS_WITH_AWS
    Status
    SetStorageConfigS3Enable(const std::string& value);
    Status
    SetStorageConfigS3Address(const std::string& value);
    Status
    SetStorageConfigS3Port(const std::string& value);
    Status
    SetStorageConfigS3AccessKey(const std::string& value);
    Status
    SetStorageConfigS3SecretKey(const std::string& value);
    Status
    SetStorageConfigS3Bucket(const std::string& value);
    Status
    SetStorageConfigS3Region(const std::string& value);
#endif

    /* metric config */
    Status
    SetMetricConfigEnableMonitor(const std::string& value);
    Status
    SetMetricConfigAddress(const std::string& value);
    Status
    SetMetricConfigPort(const std::string& value);

    /* cache config */
    Status
    SetCacheConfigCpuCacheCapacity(const std::string& value);
    Status
    SetCacheConfigCpuCacheThreshold(const std::string& value);
    Status
    SetCacheConfigInsertBufferSize(const std::string& value);
    Status
    SetCacheConfigCacheInsertData(const std::string& value);
    Status
    SetCacheConfigPreloadCollection(const std::string& value);

    /* engine config */
    Status
    SetEngineConfigUseBlasThreshold(const std::string& value);
    Status
    SetEngineConfigOmpThreadNum(const std::string& value);
    Status
    SetEngineConfigSimdType(const std::string& value);
    Status
    SetEngineSearchCombineMaxNq(const std::string& value);
    Status
    SetEngineConfigMaxPartitionNum(const std::string& value);
#ifdef MILVUS_GPU_VERSION

    /* gpu resource config */
    Status
    SetGpuResourceConfigEnable(const std::string& value);
    Status
    SetGpuResourceConfigCacheEnable(const std::string& value);
    Status
    SetGpuResourceConfigCacheCapacity(const std::string& value);
    Status
    SetGpuResourceConfigCacheThreshold(const std::string& value);
    Status
    SetGpuResourceConfigGpuSearchThreshold(const std::string& value);
    Status
    SetGpuResourceConfigSearchResources(const std::string& value);
    Status
    SetGpuResourceConfigBuildIndexResources(const std::string& value);
#endif

    /* tracing config */
    Status
    SetTracingConfigJsonConfigPath(const std::string& value);

    /* wal config */
    Status
    SetWalConfigEnable(const std::string& value);
    Status
    SetWalConfigRecoveryErrorIgnore(const std::string& value);
    Status
    SetWalConfigBufferSize(const std::string& value);
    Status
    SetWalConfigWalPath(const std::string& value);

    /* logs config */
    Status
    SetLogsLevel(const std::string& value);
    Status
    SetLogsTraceEnable(const std::string& value);
    Status
    SetLogsPath(const std::string& value);
    Status
    SetLogsMaxLogFileSize(const std::string& value);
    Status
    SetLogsLogRotateNum(const std::string& value);
    Status
    SetLogsLogToStdout(const std::string& value);
    Status
    SetLogsLogToFile(const std::string& value);

    void
    ClearCache() {
        std::lock_guard<std::mutex> lock(mutex_);
        config_map_.clear();
    }

 private:
    bool restart_required_ = false;
    std::string config_file_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> config_map_;
    std::unordered_map<std::string, std::unordered_map<std::string, ConfigCallBackF>> config_callback_;
    std::mutex callback_mutex_;
    std::mutex mutex_;
};

}  // namespace server
}  // namespace milvus
