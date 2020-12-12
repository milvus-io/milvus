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

#include "value/config/ServerConfig.h"

/* to find modifiable settings fast */
#define _MODIFIABLE (true)
#define _IMMUTABLE (false)
const int64_t MB = (1024ll * 1024);
const int64_t GB = (1024ll * 1024 * 1024);
const int64_t HOURS = (3600ll);
const int64_t DAYS = (HOURS * 24);

namespace milvus {

bool
is_nodeid_valid(const std::string& val, std::string& err) {
    // lambda, check if it's [0-9a-zA-Z_-]
    auto is_valid = [](char ch) {
        if (isalnum(ch) || ch == '_' || ch == '-') {
            return true;
        }
        return false;
    };

    for (auto& ch : val) {
        if (not is_valid(ch)) {
            err = "Invalid nodeid: " + val + ", supported char: [0-9a-zA-Z_-]";
            return false;
        }
    }
    return true;
}

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

#define Bool_(name, modifiable, default, is_valid) \
    { #name, CreateBoolValue(#name, modifiable, config.name, default, is_valid) }
#define String_(name, modifiable, default, is_valid) \
    { #name, CreateStringValue(#name, modifiable, config.name, default, is_valid) }
#define Enum_(name, modifiable, enumd, default, is_valid) \
    { #name, CreateEnumValue(#name, modifiable, enumd, config.name, default, is_valid) }
#define Integer_(name, modifiable, lower_bound, upper_bound, default, is_valid) \
    { #name, CreateIntegerValue(#name, modifiable, lower_bound, upper_bound, config.name, default, is_valid) }
#define Floating_(name, modifiable, lower_bound, upper_bound, default, is_valid) \
    { #name, CreateFloatingValue(#name, modifiable, lower_bound, upper_bound, config.name, default, is_valid) }
#define Size_(name, modifiable, lower_bound, upper_bound, default, is_valid) \
    { #name, CreateSizeValue(#name, modifiable, lower_bound, upper_bound, config.name, default, is_valid) }
#define Time_(name, modifiable, lower_bound, upper_bound, default, is_valid) \
    { #name, CreateTimeValue(#name, modifiable, lower_bound, upper_bound, config.name, default, is_valid) }

#define Bool(name, default) Bool_(name, true, default, nullptr)
#define String(name, default) String_(name, true, default, nullptr)
#define Enum(name, enumd, default) Enum_(name, true, enumd, default, nullptr)
#define Integer(name, lower_bound, upper_bound, default) \
    Integer_(name, true, lower_bound, upper_bound, default, nullptr)
#define Floating(name, lower_bound, upper_bound, default) \
    Floating_(name, true, lower_bound, upper_bound, default, nullptr)
#define Size(name, lower_bound, upper_bound, default) Size_(name, true, lower_bound, upper_bound, default, nullptr)
#define Time(name, lower_bound, upper_bound, default) Time_(name, true, lower_bound, upper_bound, default, nullptr)

std::unordered_map<std::string, BaseValuePtr>
InitConfig() {
    return std::unordered_map<std::string, BaseValuePtr>{
        /* version */
        String(version, "unknown"),

        /* cluster */
        Bool(cluster.enable, false),
        Enum(cluster.role, &ClusterRoleMap, ClusterRole::RW),
        String_(cluster.node_id, _MODIFIABLE, "master", is_nodeid_valid),

        /* general */
        String_(general.timezone, _MODIFIABLE, "UTC+8", is_timezone_valid),
        String(general.meta_uri, "sqlite://:@:/"),
        Integer(general.stale_snapshots_count, 0, 100, 0),
        Integer(general.stale_snapshots_duration, 0, std::numeric_limits<int64_t>::max(), 10),

        /* network */
        String(network.bind.address, "0.0.0.0"),
        Integer(network.bind.port, 1025, 65534, 19530),
        Bool(network.http.enable, true),
        Integer(network.http.port, 1025, 65534, 19121),

        /* storage */
        String(storage.path, "/var/lib/milvus"),
        Integer(storage.auto_flush_interval, 0, std::numeric_limits<int64_t>::max(), 1),

        /* wal */
        Bool(wal.enable, true),
        Bool(wal.sync_mode, false),
        Bool(wal.recovery_error_ignore, false),
        Size(wal.buffer_size, 64 * MB, 4096 * MB, 256 * MB),
        String(wal.path, "/var/lib/milvus/wal"),

        /* cache */
        Size_(cache.cache_size, _MODIFIABLE, 0, std::numeric_limits<int64_t>::max(), 4 * GB, is_cachesize_valid),
        Floating(cache.cpu_cache_threshold, 0.0, 1.0, 0.7),
        Size(cache.insert_buffer_size, 0, std::numeric_limits<int64_t>::max(), 1 * GB),
        Bool(cache.cache_insert_data, false),
        String(cache.preload_collection, ""),
        Size(cache.max_concurrent_insert_request_size, 256 * MB, std::numeric_limits<int64_t>::max(), 2 * GB),

        /* gpu */
        Bool(gpu.enable, false),
        Size(gpu.cache_size, 0, std::numeric_limits<int64_t>::max(), 1 * GB),
        Floating(gpu.cache_threshold, 0.0, 1.0, 0.7),
        Integer(gpu.gpu_search_threshold, 0, std::numeric_limits<int64_t>::max(), 1000),
        String(gpu.search_devices, "gpu0"),
        String(gpu.build_index_devices, "gpu0"),

        /* log */
        Bool(logs.trace.enable, true),
        String(logs.path, "/var/lib/milvus/logs"),
        Size(logs.max_log_file_size, 512 * MB, 4096 * MB, 1024 * MB),
        Integer(logs.log_rotate_num, 0, 1024, 0),
        Bool(logs.log_to_stdout, false),
        Bool(logs.log_to_file, true),

        String(log.min_messages, "warning"),
        Time(log.rotation_age, 0, 16384ll * HOURS, 24ll * HOURS),
        Size(log.rotation_size, 128 * MB, 8192 * MB, 1024 * MB),

        /* tracing */
        String(tracing.json_config_path, ""),

        /* invisible */
        /* engine */
        Integer(engine.max_partition_num, 1, std::numeric_limits<int64_t>::max(), 4096),
        Integer(engine.build_index_threshold, 0, std::numeric_limits<int64_t>::max(), 4096),
        Integer(engine.search_combine_nq, 0, std::numeric_limits<int64_t>::max(), 64),
        Integer(engine.use_blas_threshold, 0, std::numeric_limits<int64_t>::max(), 16385),
        Integer(engine.omp_thread_num, 0, std::numeric_limits<int64_t>::max(), 0),
        Enum(engine.clustering_type, &ClusteringMap, ClusteringType::K_MEANS),
        Enum(engine.simd_type, &SimdMap, SimdType::AUTO),
        Integer(engine.statistics_level, 0, 3, 1),

        Bool(system.lock.enable, true),

        Bool(transcript.enable, false),
        String(transcript.replay, ""),
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
# node_id              | Node ID, used in log message only.                         | String     | master          |
#----------------------+------------------------------------------------------------+------------+-----------------+
cluster:
  enable: @cluster.enable@
  role: @cluster.role@
  node_id: @cluster.node_id@

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
# stale_snapshots_count| Specify how many stale snapshots to be kept before GC. It  | Integer    |  0              |
#                      | is ignored if deployed with cluster enabled                |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# stale_snapshots_duration |                                                        | Integer    |  10             |
#                      | Specify how long the stale snapshots can be GC'ed. The unit|            |                 |
#                      | is second. It is only effective if deployed with cluster   |            |                 |
#                      | enabled and cluster.role is rw                             |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
general:
  timezone: @general.timezone@
  meta_uri: @general.meta_uri@
  stale_snapshots_count: @general.stale_snapshots_count@
  stale_snapshots_duration: @general.stale_snapshots_duration@

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
# max_concurrent_insert_request_size |                                              |            |                 |
#                      | A size limit on the concurrent insert requests to process. | String     | 2GB             |
#                      | Milvus can process insert requests from multiple clients   |            |                 |
#                      | concurrently. This setting puts a cap on the memory        |            |                 |
#                      | consumption during this process.                           |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
cache:
  cache_size: @cache.cache_size@
  insert_buffer_size: @cache.insert_buffer_size@
  preload_collection: @cache.preload_collection@
  max_concurrent_insert_request_size: @cache.max_concurrent_insert_request_size@

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
# log_to_stdout        | Whether logging to standard output.                        | Boolean    | false           |
#----------------------+------------------------------------------------------------+------------+-----------------+
# log_to_file          | Whether logging to log files.                              | Boolean    | true            |
#----------------------+------------------------------------------------------------+------------+-----------------+
logs:
  trace.enable: @logs.trace.enable@
  path: @logs.path@
  max_log_file_size: @logs.max_log_file_size@
  log_rotate_num: @logs.log_rotate_num@
  log_to_stdout: @logs.log_to_stdout@
  log_to_file: @logs.log_to_file@

#----------------------+------------------------------------------------------------+------------+-----------------+
# Log Config           | Description                                                | Type       | Default         |
#----------------------+------------------------------------------------------------+------------+-----------------+
# min_messages         | Log level in Milvus. Must be one of debug, info, warning,  | String     | warning         |
#                      | error, fatal                                               |            |                 |
#----------------------+------------------------------------------------------------+------------+-----------------+
# rotation_age         | When to generate new logfile.                              | Time       | 24 hours        |
#----------------------+------------------------------------------------------------+------------+-----------------+
# rotation_size        | When to generate new logfile.                              | Size       | 1GB             |
#----------------------+------------------------------------------------------------+------------+-----------------+
log:
  min_messages: @log.min_messages@
  rotation_age: @log.rotation_age@
  rotation_size: @log.rotation_size@

)";

}  // namespace milvus
