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

#include <sys/sysinfo.h>
#include <fstream>

#include "config/ServerConfig.h"

/* to find modifiable settings fast */
#define _MODIFIABLE (true)
#define _IMMUTABLE (false)
const int64_t MB = (1024ll * 1024);
const int64_t GB = (1024ll * 1024 * 1024);

namespace milvus {

bool
is_timezone_valid(const std::string& val, std::string& err) {
    auto plus_count = std::count(val.begin(), val.end(), '+');
    auto sub_count = std::count(val.begin(), val.end(), '-');
    if (plus_count > 1 or sub_count > 1) {
        err = "Invalid timezone: " + val;
        return false;
    }
    return true;
}

bool
is_cachesize_valid(int64_t size, std::string& err) {
    try {
        // Get max docker memory size
        int64_t limit_in_bytes;
        std::ifstream file("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        if (file.fail()) {
            throw std::runtime_error("Failed to read /sys/fs/cgroup/memory/memory.limit_in_bytes.");
        }
        file >> limit_in_bytes;

        // Get System info
        int64_t total_mem = 0;
        struct sysinfo info;
        int ret = sysinfo(&info);
        if (ret != 0) {
            throw std::runtime_error("Get sysinfo failed.");
        }
        total_mem = info.totalram;

        if (limit_in_bytes < total_mem && size > limit_in_bytes) {
            std::string msg =
                "Invalid cpu cache size: " + std::to_string(size) +
                ". cache.cache_size exceeds system cgroup memory size: " + std::to_string(limit_in_bytes) + "." +
                "Consider increase docker memory limit.";
            throw std::runtime_error(msg);
        }
        return true;
    } catch (std::exception& ex) {
        err = "Check cache.cache_size valid failed, reason: " + std::string(ex.what());
        return false;
    } catch (...) {
        err = "Check cache.cache_size valid failed, unknown reason.";
        return false;
    }
}

std::unordered_map<std::string, BaseConfigPtr>
InitConfig() {
    return std::unordered_map<std::string, BaseConfigPtr>{
        /* version */
        {"version", CreateStringConfig("version", &config.version.value, "unknown")},

        /* cluster */
        {"cluster.enable", CreateBoolConfig("cluster.enable", &config.cluster.enable.value, false)},
        {"cluster.role",
         CreateEnumConfig("cluster.role", &ClusterRoleMap, &config.cluster.role.value, ClusterRole::RW)},

        /* general */
        {"general.timezone", CreateStringConfig_("general.timezone", _MODIFIABLE, &config.general.timezone.value,
                                                 "UTC+8", is_timezone_valid, nullptr)},
        {"general.meta_uri", CreateStringConfig("general.meta_uri", &config.general.meta_uri.value, "sqlite://:@:/")},

        /* network */
        {"network.bind.address",
         CreateStringConfig("network.bind.address", &config.network.bind.address.value, "0.0.0.0")},
        {"network.bind.port",
         CreateIntegerConfig("network.bind.port", 1025, 65534, &config.network.bind.port.value, 19530)},
        {"network.http.enable", CreateBoolConfig("network.http.enable", &config.network.http.enable.value, true)},
        {"network.http.port",
         CreateIntegerConfig("network.http.port", 1025, 65534, &config.network.http.port.value, 19121)},

        /* storage */
        {"storage.path", CreateStringConfig("storage.path", &config.storage.path.value, "/var/lib/milvus")},
        {"storage.auto_flush_interval",
         CreateIntegerConfig("storage.auto_flush_interval", 0, std::numeric_limits<int64_t>::max(),
                             &config.storage.auto_flush_interval.value, 1)},

        /* wal */
        {"wal.enable", CreateBoolConfig("wal.enable", &config.wal.enable.value, true)},
        {"wal.recovery_error_ignore",
         CreateBoolConfig("wal.recovery_error_ignore", &config.wal.recovery_error_ignore.value, false)},
        {"wal.buffer_size",
         CreateSizeConfig("wal.buffer_size", 64 * MB, 4096 * MB, &config.wal.buffer_size.value, 256 * MB)},
        {"wal.path", CreateStringConfig("wal.path", &config.wal.path.value, "/var/lib/milvus/wal")},

        /* cache */
        {"cache.cache_size", CreateSizeConfig_("cache.cache_size", _MODIFIABLE, 0, std::numeric_limits<int64_t>::max(),
                                               &config.cache.cache_size.value, 4 * GB, is_cachesize_valid, nullptr)},
        {"cache.cpu_cache_threshold",
         CreateFloatingConfig("cache.cpu_cache_threshold", 0.0, 1.0, &config.cache.cpu_cache_threshold.value, 0.7)},
        {"cache.insert_buffer_size",
         CreateSizeConfig("cache.insert_buffer_size", 0, std::numeric_limits<int64_t>::max(),
                          &config.cache.insert_buffer_size.value, 1 * GB)},
        {"cache.cache_insert_data",
         CreateBoolConfig("cache.cache_insert_data", &config.cache.cache_insert_data.value, false)},
        {"cache.preload_collection",
         CreateStringConfig("cache.preload_collection", &config.cache.preload_collection.value, "")},

        /* gpu */
        {"gpu.enable", CreateBoolConfig("gpu.enable", &config.gpu.enable.value, false)},
        {"gpu.cache_size", CreateSizeConfig("gpu.cache_size", 0, std::numeric_limits<int64_t>::max(),
                                            &config.gpu.cache_size.value, 1 * GB)},
        {"gpu.cache_threshold",
         CreateFloatingConfig("gpu.cache_threshold", 0.0, 1.0, &config.gpu.cache_threshold.value, 0.7)},
        {"gpu.gpu_search_threshold",
         CreateIntegerConfig("gpu.gpu_search_threshold", 0, std::numeric_limits<int64_t>::max(),
                             &config.gpu.gpu_search_threshold.value, 1000)},
        {"gpu.search_devices", CreateStringConfig("gpu.search_devices", &config.gpu.search_devices.value, "gpu0")},
        {"gpu.build_index_devices",
         CreateStringConfig("gpu.build_index_devices", &config.gpu.build_index_devices.value, "gpu0")},

        /* log */
        {"logs.level", CreateStringConfig("logs.level", &config.logs.level.value, "debug")},
        {"logs.trace.enable", CreateBoolConfig("logs.trace.enable", &config.logs.trace.enable.value, true)},
        {"logs.path", CreateStringConfig("logs.path", &config.logs.path.value, "/var/lib/milvus/logs")},
        {"logs.max_log_file_size", CreateSizeConfig("logs.max_log_file_size", 512 * MB, 4096 * MB,
                                                    &config.logs.max_log_file_size.value, 1024 * MB)},
        {"logs.log_rotate_num",
         CreateIntegerConfig("logs.log_rotate_num", 0, 1024, &config.logs.log_rotate_num.value, 0)},

        /* metric */
        {"metric.enable", CreateBoolConfig("metric.enable", &config.metric.enable.value, false)},
        {"metric.address", CreateStringConfig("metric.address", &config.metric.address.value, "127.0.0.1")},
        {"metric.port", CreateIntegerConfig("metric.port", 1025, 65534, &config.metric.port.value, 9091)},

        /* tracing */
        {"tracing.json_config_path",
         CreateStringConfig("tracing.json_config_path", &config.tracing.json_config_path.value, "")},

        /* invisible */
        /* engine */
        {"engine.build_index_threshold",
         CreateIntegerConfig("engine.build_index_threshold", 0, std::numeric_limits<int64_t>::max(),
                             &config.engine.build_index_threshold.value, 4096)},
        {"engine.search_combine_nq",
         CreateIntegerConfig("engine.search_combine_nq", 0, std::numeric_limits<int64_t>::max(),
                             &config.engine.search_combine_nq.value, 64)},
        {"engine.use_blas_threshold",
         CreateIntegerConfig("engine.use_blas_threshold", 0, std::numeric_limits<int64_t>::max(),
                             &config.engine.use_blas_threshold.value, 1100)},
        {"engine.omp_thread_num", CreateIntegerConfig("engine.omp_thread_num", 0, std::numeric_limits<int64_t>::max(),
                                                      &config.engine.omp_thread_num.value, 0)},
        {"engine.clustering_type", CreateEnumConfig("engine.clustering_type", &ClusteringMap,
                                                    &config.engine.clustering_type.value, ClusteringType::K_MEANS)},
        {"engine.simd_type",
         CreateEnumConfig("engine.simd_type", &SimdMap, &config.engine.simd_type.value, SimdType::AUTO)},

        {"system.lock.enable", CreateBoolConfig("system.lock.enable", &config.system.lock.enable.value, true)},

        {"transcript.enable", CreateBoolConfig("transcript.enable", &config.transcript.enable.value, false)},
        {"transcript.replay", CreateStringConfig("transcript.replay", &config.transcript.replay.value, "")},
    };
}

const char* config_file_template = R"(
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

version: @version@

#----------------------+------------------------------------------------------------+------------+-----------------+
# Cluster Config       | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# enable               | If running with Mishards, set true, otherwise false.       | Boolean    | false           |
#----------------------+------------------------------------------------------------+------------+-----------------+
# role                 | Milvus deployment role: rw / ro                            | Role       | rw              |
#----------------------+------------------------------------------------------------+------------+-----------------+
cluster:
  enable: @cluster.enable@
  role: @cluster.role@

#----------------------+------------------------------------------------------------+------------+-----------------+
# General Config       | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# timezone             | Use UTC-x or UTC+x to specify a time zone.                 | Timezone   | UTC+8           |
#----------------------+------------------------------------------------------------+------------+-----------------+
# meta_uri             | URI for metadata storage, using SQLite (for single server  | URI        | sqlite://:@:/   |
#                      | Milvus) or MySQL (for distributed cluster Milvus).         |            |                 |
#                      | Format: dialect://username:password@host:port/database     |            |                 |
#                      | Keep 'dialect://:@:/', 'dialect' can be either 'sqlite' or |            |                 |
#                      | 'mysql', replace other texts with real values.             |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
general:
  timezone: @general.timezone@
  meta_uri: @general.meta_uri@

#----------------------+------------------------------------------------------------+------------+-----------------+
# Network Config       | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# bind.address         | IP address that Milvus server monitors.                    | IP         | 0.0.0.0         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# bind.port            | Port that Milvus server monitors. Port range (1024, 65535) | Integer    | 19530           |
#----------------------+------------------------------------------------------------+------------+-----------------+
# http.enable          | Enable HTTP server or not.                                 | Boolean    | true            |
#----------------------+------------------------------------------------------------+------------+-----------------+
# http.port            | Port that Milvus HTTP server monitors.                     | Integer    | 19121           |
#                      | Port range (1024, 65535)                                   |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
network:
  bind.address: @network.bind.address@
  bind.port: @network.bind.port@
  http.enable: @network.http.enable@
  http.port: @network.http.port@

#----------------------+------------------------------------------------------------+------------+-----------------+
# Storage Config       | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# path                 | Path used to save meta data, vector data and index data.   | Path       | /var/lib/milvus |
#----------------------+------------------------------------------------------------+------------+-----------------+
# auto_flush_interval  | The interval, in seconds, at which Milvus automatically    | Integer    | 1 (s)           |
#                      | flushes data to disk.                                      |            |                 |
#                      | 0 means disable the regular flush.                         |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
storage:
  path: @storage.path@
  auto_flush_interval: @storage.auto_flush_interval@

#----------------------+------------------------------------------------------------+------------+-----------------+
# WAL Config           | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# enable               | Whether to enable write-ahead logging (WAL) in Milvus.     | Boolean    | true            |
#                      | If WAL is enabled, Milvus writes all data changes to log   |            |                 |
#                      | files in advance before implementing data changes. WAL     |            |                 |
#                      | ensures the atomicity and durability for Milvus operations.|            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# path                 | Location of WAL log files.                                 | String     |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
wal:
  enable: @wal.enable@
  path: @wal.path@

#----------------------+------------------------------------------------------------+------------+-----------------+
# Cache Config         | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# cache_size           | The size of CPU memory used for caching data for faster    | String     | 4GB             |
#                      | query. The sum of 'cache_size' and 'insert_buffer_size'    |            |                 |
#                      | must be less than system memory size.                      |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# insert_buffer_size   | Buffer size used for data insertion.                       | String     | 1GB             |
#                      | The sum of 'insert_buffer_size' and 'cache_size'           |            |                 |
#                      | must be less than system memory size.                      |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# preload_collection   | A comma-separated list of collection names that need to    | StringList |                 |
#                      | be pre-loaded when Milvus server starts up.                |            |                 |
#                      | '*' means preload all existing tables (single-quote or     |            |                 |
#                      | double-quote required).                                    |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
cache:
  cache_size: @cache.cache_size@
  insert_buffer_size: @cache.insert_buffer_size@
  preload_collection: @cache.preload_collection@

#----------------------+------------------------------------------------------------+------------+-----------------+
# GPU Config           | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# enable               | Use GPU devices or not.                                    | Boolean    | false           |
#----------------------+------------------------------------------------------------+------------+-----------------+
# cache_size           | The size of GPU memory per card used for cache.            | String     | 1GB             |
#----------------------+------------------------------------------------------------+------------+-----------------+
# gpu_search_threshold | A Milvus performance tuning parameter. This value will be  | Integer    | 1000            |
#                      | compared with 'nq' to decide if the search computation will|            |                 |
#                      | be executed on GPUs only.                                  |            |                 |
#                      | If nq >= gpu_search_threshold, the search computation will |            |                 |
#                      | be executed on GPUs only;                                  |            |                 |
#                      | if nq < gpu_search_threshold, the search computation will  |            |                 |
#                      | be executed on both CPUs and GPUs.                         |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# search_devices       | The list of GPU devices used for search computation.       | DeviceList | gpu0            |
#                      | Must be in format gpux.                                    |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# build_index_devices  | The list of GPU devices used for index building.           | DeviceList | gpu0            |
#                      | Must be in format gpux.                                    |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
gpu:
  enable: @gpu.enable@
  cache_size: @gpu.cache_size@
  gpu_search_threshold: @gpu.gpu_search_threshold@
  search_devices: @gpu.search_devices@
  build_index_devices: @gpu.build_index_devices@

#----------------------+------------------------------------------------------------+------------+-----------------+
# Logs Config          | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# level                | Log level in Milvus. Must be one of debug, info, warning,  | String     | debug           |
#                      | error, fatal                                               |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# trace.enable         | Whether to enable trace level logging in Milvus.           | Boolean    | true            |
#----------------------+------------------------------------------------------------+------------+-----------------+
# path                 | Absolute path to the folder holding the log files.         | String     |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# max_log_file_size    | The maximum size of each log file, size range              | String     | 1024MB          |
#                      | [512MB, 4096MB].                                           |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# log_rotate_num       | The maximum number of log files that Milvus keeps for each | Integer    | 0               |
#                      | logging level, num range [0, 1024], 0 means unlimited.     |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
logs:
  level: @logs.level@
  trace.enable: @logs.trace.enable@
  path: @logs.path@
  max_log_file_size: @logs.max_log_file_size@
  log_rotate_num: @logs.log_rotate_num@

#----------------------+------------------------------------------------------------+------------+-----------------+
# Metric Config        | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# enable               | Enable monitoring function or not.                         | Boolean    | false           |
#----------------------+------------------------------------------------------------+------------+-----------------+
# address              | Pushgateway address                                        | IP         | 127.0.0.1       +
#----------------------+------------------------------------------------------------+------------+-----------------+
# port                 | Pushgateway port, port range (1024, 65535)                 | Integer    | 9091            |
#----------------------+------------------------------------------------------------+------------+-----------------+
metric:
  enable: @metric.enable@
  address: @metric.address@
  port: @metric.port@

)";

}  // namespace milvus
