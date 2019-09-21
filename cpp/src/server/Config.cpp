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

#include "Config.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>

#include "config/ConfigMgr.h"
#include "utils/CommonUtil.h"
#include "utils/ValidationUtil.h"


namespace zilliz {
namespace milvus {
namespace server {

constexpr uint64_t MB = 1024 * 1024;
constexpr uint64_t GB = MB * 1024;

Config &
Config::GetInstance() {
    static Config config_inst;
    return config_inst;
}

Status
Config::LoadConfigFile(const std::string &filename) {
    if (filename.empty()) {
        std::cerr << "ERROR: need specify config file" << std::endl;
        exit(1);
    }
    struct stat dirStat;
    int statOK = stat(filename.c_str(), &dirStat);
    if (statOK != 0) {
        std::cerr << "ERROR: Config file not exist: " << filename << std::endl;
        exit(1);
    }

    try {
        ConfigMgr *mgr = const_cast<ConfigMgr *>(ConfigMgr::GetInstance());
        ErrorCode err = mgr->LoadConfigFile(filename);
        if (err != 0) {
            std::cerr << "Server failed to load config file: " << filename << std::endl;
            exit(1);
        }
    }
    catch (YAML::Exception &e) {
        std::cerr << "Server failed to load config file: " << filename << std::endl;
        exit(1);
    }

    return Status::OK();
}

Status
Config::ValidateConfig() {
    if (!CheckServerConfig().ok()) {
        return Status(SERVER_INVALID_ARGUMENT, "Server config validation check fail");
    }
    if (!CheckDBConfig().ok()) {
        return Status(SERVER_INVALID_ARGUMENT, "DB config validation check fail");
    }
    if (!CheckMetricConfig().ok()) {
        return Status(SERVER_INVALID_ARGUMENT, "Metric config validation check fail");
    }
    if (!CheckCacheConfig().ok()) {
        return Status(SERVER_INVALID_ARGUMENT, "Cache config validation check fail");
    }
    if (!CheckEngineConfig().ok()) {
        return Status(SERVER_INVALID_ARGUMENT, "Engine config validation check fail");
    }
    if (!CheckResourceConfig().ok()) {
        return Status(SERVER_INVALID_ARGUMENT, "Resource config validation check fail");
    }
    return Status::OK();
}

Status
Config::CheckServerConfig() {
/*
  server_config:
  address: 0.0.0.0            # milvus server ip address (IPv4)
  port: 19530                 # the port milvus listen to, default: 19530, range: 1025 ~ 65534
  mode: single                # milvus deployment type: single, cluster, read_only
  time_zone: UTC+8            # Use the UTC-x or UTC+x to specify a time zone. eg. UTC+8 for China Standard Time

*/
    bool okay = true;
    ConfigNode server_config = GetConfig(CONFIG_SERVER);

    std::string ip_address = server_config.GetValue(CONFIG_SERVER_ADDRESS, CONFIG_SERVER_ADDRESS_DEFAULT);
    if (!ValidationUtil::ValidateIpAddress(ip_address).ok()) {
        std::cerr << "ERROR: invalid server IP address: " << ip_address << std::endl;
        okay = false;
    }

    std::string port_str = server_config.GetValue(CONFIG_SERVER_PORT, CONFIG_SERVER_PORT_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(port_str).ok()) {
        std::cerr << "ERROR: port " << port_str << " is not a number" << std::endl;
        okay = false;
    } else {
        int32_t port = std::stol(port_str);
        if (!(port > 1024 && port < 65535)) {
            std::cerr << "ERROR: port " << port_str << " out of range (1024, 65535)" << std::endl;
            okay = false;
        }
    }

    std::string mode = server_config.GetValue(CONFIG_SERVER_MODE, CONFIG_SERVER_MODE_DEFAULT);
    if (mode != "single" && mode != "cluster" && mode != "read_only") {
        std::cerr << "ERROR: mode " << mode << " is not one of ['single', 'cluster', 'read_only']" << std::endl;
        okay = false;
    }

    std::string time_zone = server_config.GetValue(CONFIG_SERVER_TIME_ZONE, CONFIG_SERVER_TIME_ZONE_DEFAULT);
    int flag = 0;
    if (time_zone.length() <= 3) {
        flag = 1;
    } else {
        if (time_zone.substr(0, 3) != "UTC") {
            flag = 1;
        } else {
            try {
                stoi(time_zone.substr(3));
            } catch (...) {
                flag = 1;
            }
        }
    }
    if (flag == 1) {
        std::cerr << "ERROR: time_zone " << time_zone << " format wrong" << std::endl;
        okay = false;
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Illegal server config"));
}

Status
Config::CheckDBConfig() {
/*
  db_config:
  db_path: @MILVUS_DB_PATH@             # milvus data storage path
  db_slave_path:                        # secondry data storage path, split by semicolon

  # URI format: dialect://username:password@host:port/database
  # All parts except dialect are optional, but you MUST include the delimiters
  # Currently dialect supports mysql or sqlite
  db_backend_url: sqlite://:@:/

  archive_disk_threshold: 0        # triger archive action if storage size exceed this value, 0 means no limit, unit: GB
  archive_days_threshold: 0        # files older than x days will be archived, 0 means no limit, unit: day
  insert_buffer_size: 4            # maximum insert buffer size allowed, default: 4, unit: GB, should be at least 1 GB.
                                   # the sum of insert_buffer_size and cpu_cache_capacity should be less than total memory, unit: GB
  build_index_gpu: 0               # which gpu is used to build index, default: 0, range: 0 ~ gpu number - 1
*/
    bool okay = true;
    ConfigNode db_config = GetConfig(CONFIG_DB);

    std::string db_path = db_config.GetValue(CONFIG_DB_PATH);
    if (db_path.empty()) {
        std::cerr << "ERROR: db_path is empty" << std::endl;
        okay = false;
    }

    std::string db_backend_url = db_config.GetValue(CONFIG_DB_BACKEND_URL);
    if (!ValidationUtil::ValidateDbURI(db_backend_url).ok()) {
        std::cerr << "ERROR: invalid db_backend_url: " << db_backend_url << std::endl;
        okay = false;
    }

    std::string archive_disk_threshold_str = db_config.GetValue(CONFIG_DB_ARCHIVE_DISK_THRESHOLD, CONFIG_DB_ARCHIVE_DISK_THRESHOLD_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(archive_disk_threshold_str).ok()) {
        std::cerr << "ERROR: archive_disk_threshold " << archive_disk_threshold_str << " is not a number" << std::endl;
        okay = false;
    }

    std::string archive_days_threshold_str = db_config.GetValue(CONFIG_DB_ARCHIVE_DAYS_THRESHOLD, CONFIG_DB_ARCHIVE_DAYS_THRESHOLD_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(archive_days_threshold_str).ok()) {
        std::cerr << "ERROR: archive_days_threshold " << archive_days_threshold_str << " is not a number" << std::endl;
        okay = false;
    }

    std::string buffer_size_str = db_config.GetValue(CONFIG_DB_BUFFER_SIZE, CONFIG_DB_BUFFER_SIZE_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(buffer_size_str).ok()) {
        std::cerr << "ERROR: buffer_size " << buffer_size_str << " is not a number" << std::endl;
        okay = false;
    }
    else {
        uint64_t buffer_size = (uint64_t) std::stol(buffer_size_str);
        buffer_size *= GB;
        unsigned long total_mem = 0, free_mem = 0;
        CommonUtil::GetSystemMemInfo(total_mem, free_mem);
        if (buffer_size >= total_mem) {
            std::cerr << "ERROR: buffer_size exceed system memory" << std::endl;
            okay = false;
        }
    }

    std::string gpu_index_str = db_config.GetValue(CONFIG_DB_BUILD_INDEX_GPU, CONFIG_DB_BUILD_INDEX_GPU_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(gpu_index_str).ok()) {
        std::cerr << "ERROR: gpu_index " << gpu_index_str << " is not a number" << std::endl;
        okay = false;
    } else {
        int32_t gpu_index = std::stol(gpu_index_str);
        if (!ValidationUtil::ValidateGpuIndex(gpu_index).ok()) {
            std::cerr << "ERROR: invalid gpu_index " << gpu_index_str << std::endl;
            okay = false;
        }
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "DB config is illegal"));
}

Status
Config::CheckMetricConfig() {
/*
    metric_config:
    is_startup: off                       # if monitoring start: on, off
    collector: prometheus                 # metrics collector: prometheus
    prometheus_config:                    # following are prometheus configure
    port: 8080                          # the port prometheus use to fetch metrics
    (not used) push_gateway_ip_address: 127.0.0.1  # push method configure: push gateway ip address
    (not used) push_gateway_port: 9091             # push method configure: push gateway port
*/
    bool okay = true;
    ConfigNode metric_config = GetConfig(CONFIG_METRIC);

    std::string is_startup_str =
        metric_config.GetValue(CONFIG_METRIC_AUTO_BOOTUP, CONFIG_METRIC_AUTO_BOOTUP_DEFAULT);
    if (!ValidationUtil::ValidateStringIsBool(is_startup_str).ok()) {
        std::cerr << "ERROR: invalid is_startup config: " << is_startup_str << std::endl;
        okay = false;
    }

    std::string port_str = metric_config.GetChild(CONFIG_METRIC_PROMETHEUS).GetValue(CONFIG_METRIC_PROMETHEUS_PORT, "8080");
    if (!ValidationUtil::ValidateStringIsNumber(port_str).ok()) {
        std::cerr << "ERROR: port specified in prometheus_config " << port_str << " is not a number" << std::endl;
        okay = false;
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Metric config is illegal"));
}

Status
Config::CheckCacheConfig() {
/*
  cache_config:
  cpu_cache_capacity: 16            # how many memory are used as cache, unit: GB, range: 0 ~ less than total memory
  cpu_cache_free_percent: 0.85      # old data will be erased from cache when cache is full, this value specify how much memory should be kept, range: greater than zero ~ 1.0
  insert_cache_immediately: false   # insert data will be load into cache immediately for hot query
  gpu_cache_capacity: 5             # how many memory are used as cache in gpu, unit: GB, RANGE: 0 ~ less than total memory
  gpu_cache_free_percent: 0.85      # old data will be erased from cache when cache is full, this value specify how much memory should be kept, range: greater than zero ~ 1.0

*/
    bool okay = true;
    ConfigNode cache_config = GetConfig(CONFIG_CACHE);

    std::string cpu_cache_capacity_str =
        cache_config.GetValue(CONFIG_CACHE_CPU_MEM_CAPACITY, CONFIG_CACHE_CPU_MEM_CAPACITY_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(cpu_cache_capacity_str).ok()) {
        std::cerr << "ERROR: cpu_cache_capacity " << cpu_cache_capacity_str << " is not a number" << std::endl;
        okay = false;
    }
    else {
        uint64_t cpu_cache_capacity = (uint64_t) std::stol(cpu_cache_capacity_str);
        cpu_cache_capacity *= GB;
        unsigned long total_mem = 0, free_mem = 0;
        CommonUtil::GetSystemMemInfo(total_mem, free_mem);
        if (cpu_cache_capacity >= total_mem) {
            std::cerr << "ERROR: cpu_cache_capacity exceed system memory" << std::endl;
            okay = false;
        }
        else if (cpu_cache_capacity > (double) total_mem * 0.9) {
            std::cerr << "Warning: cpu_cache_capacity value is too aggressive" << std::endl;
        }

        uint64_t buffer_size =
            (uint64_t) GetConfig(CONFIG_DB).GetInt32Value(CONFIG_DB_BUFFER_SIZE, std::stoi(CONFIG_DB_BUFFER_SIZE_DEFAULT));
        buffer_size *= GB;
        if (buffer_size + cpu_cache_capacity >= total_mem) {
            std::cerr << "ERROR: sum of cpu_cache_capacity and insert_buffer_size exceed system memory" << std::endl;
            okay = false;
        }
    }

    std::string cpu_cache_free_percent_str =
        cache_config.GetValue(CONFIG_CACHE_CPU_MEM_THRESHOLD, CONFIG_CACHE_CPU_MEM_THRESHOLD_DEFAULT);
    double cpu_cache_free_percent;
    if (!ValidationUtil::ValidateStringIsDouble(cpu_cache_free_percent_str, cpu_cache_free_percent).ok()) {
        std::cerr << "ERROR: cpu_cache_free_percent " << cpu_cache_free_percent_str << " is not a double" << std::endl;
        okay = false;
    }
    else if (cpu_cache_free_percent < std::numeric_limits<double>::epsilon() || cpu_cache_free_percent > 1.0) {
        std::cerr << "ERROR: invalid cpu_cache_free_percent " << cpu_cache_free_percent_str << std::endl;
        okay = false;
    }

    std::string insert_cache_immediately_str =
        cache_config.GetValue(CONFIG_CACHE_CACHE_INSERT_DATA, CONFIG_CACHE_CACHE_INSERT_DATA_DEFAULT);
    if (!ValidationUtil::ValidateStringIsBool(insert_cache_immediately_str).ok()) {
        std::cerr << "ERROR: invalid insert_cache_immediately config: " << insert_cache_immediately_str << std::endl;
        okay = false;
    }

    std::string gpu_cache_capacity_str =
        cache_config.GetValue(CONFIG_CACHE_GPU_MEM_CAPACITY, CONFIG_CACHE_GPU_MEM_CAPACITY_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(gpu_cache_capacity_str).ok()) {
        std::cerr << "ERROR: gpu_cache_capacity " << gpu_cache_capacity_str << " is not a number" << std::endl;
        okay = false;
    }
    else {
        uint64_t gpu_cache_capacity = (uint64_t) std::stol(gpu_cache_capacity_str);
        gpu_cache_capacity *= GB;
        int gpu_index = GetConfig(CONFIG_DB).GetInt32Value(CONFIG_DB_BUILD_INDEX_GPU, std::stoi(CONFIG_DB_BUILD_INDEX_GPU_DEFAULT));
        size_t gpu_memory;
        if (!ValidationUtil::GetGpuMemory(gpu_index, gpu_memory).ok()) {
            std::cerr << "ERROR: could not get gpu memory for device " << gpu_index << std::endl;
            okay = false;
        }
        else if (gpu_cache_capacity >= gpu_memory) {
            std::cerr << "ERROR: gpu_cache_capacity " << gpu_cache_capacity
                      << " exceed total gpu memory " << gpu_memory << std::endl;
            okay = false;
        }
        else if (gpu_cache_capacity > (double) gpu_memory * 0.9) {
            std::cerr << "Warning: gpu_cache_capacity value is too aggressive" << std::endl;
        }
    }

    std::string gpu_cache_free_percent_str =
        cache_config.GetValue(CONFIG_CACHE_GPU_MEM_THRESHOLD, CONFIG_CACHE_GPU_MEM_THRESHOLD_DEFAULT);
    double gpu_cache_free_percent;
    if (!ValidationUtil::ValidateStringIsDouble(gpu_cache_free_percent_str, gpu_cache_free_percent).ok()) {
        std::cerr << "ERROR: gpu_cache_free_percent " << gpu_cache_free_percent_str << " is not a double" << std::endl;
        okay = false;
    }
    else if (gpu_cache_free_percent < std::numeric_limits<double>::epsilon() || gpu_cache_free_percent > 1.0) {
        std::cerr << "ERROR: invalid gpu_cache_free_percent " << gpu_cache_free_percent << std::endl;
        okay = false;
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Cache config is illegal"));
}

Status
Config::CheckEngineConfig() {
/*
    engine_config:
    use_blas_threshold: 20
    omp_thread_num: 0             # how many compute threads be used by engine, 0 means use all cpu core to compute
*/
    bool okay = true;
    ConfigNode engine_config = GetConfig(CONFIG_ENGINE);

    std::string use_blas_threshold_str =
        engine_config.GetValue(CONFIG_ENGINE_BLAS_THRESHOLD, CONFIG_ENGINE_BLAS_THRESHOLD_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(use_blas_threshold_str).ok()) {
        std::cerr << "ERROR: use_blas_threshold " << use_blas_threshold_str << " is not a number" << std::endl;
        okay = false;
    }

    std::string omp_thread_num_str =
        engine_config.GetValue(CONFIG_ENGINE_OMP_THREAD_NUM, CONFIG_ENGINE_OMP_THREAD_NUM_DEFAULT);
    if (!ValidationUtil::ValidateStringIsNumber(omp_thread_num_str).ok()) {
        std::cerr << "ERROR: omp_thread_num " << omp_thread_num_str << " is not a number" << std::endl;
        okay = false;
    } else {
        int32_t omp_thread = std::stol(omp_thread_num_str);
        uint32_t sys_thread_cnt = 8;
        if (omp_thread > CommonUtil::GetSystemAvailableThreads(sys_thread_cnt)) {
            std::cerr << "ERROR: omp_thread_num " << omp_thread_num_str << " > system available thread "
                      << sys_thread_cnt << std::endl;
            okay = false;
        }
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Engine config is illegal"));
}

Status
Config::CheckResourceConfig() {
    /*
      resource_config:
        mode: simple
        pool:
          - cpu
          - gpu0
          - gpu100
     */
    bool okay = true;
    server::ConfigNode &config = GetConfig(server::CONFIG_RESOURCE);
    auto mode = config.GetValue(CONFIG_RESOURCE_MODE, CONFIG_RESOURCE_MODE_DEFAULT);
    if (mode != "simple") {
        std::cerr << "ERROR: invalid resource config: mode is " << mode << std::endl;
        okay = false;
    }
    auto pool = config.GetSequence(CONFIG_RESOURCE_POOL);
    if (pool.empty()) {
        std::cerr << "ERROR: invalid resource config: resources empty" << std::endl;
        okay = false;
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Resource config is illegal"));
}

//Status
//Config::CheckResourceConfig() {
/*
  resource_config:
  # resource list, length: 0~N
  # please set a DISK resource and a CPU resource least, or system will not return query result.
  #
  # example:
  # resource_name:               # resource name, just using in connections below
  #   type: DISK                 # resource type, optional: DISK/CPU/GPU
  #   device_id: 0
  #   enable_executor: false     # if is enable executor, optional: true, false

  resources:
    ssda:
      type: DISK
      device_id: 0
      enable_executor: false

    cpu:
      type: CPU
      device_id: 0
      enable_executor: true

    gpu0:
      type: GPU
      device_id: 0
      enable_executor: false
      gpu_resource_num: 2
      pinned_memory: 300
      temp_memory: 300

  # connection list, length: 0~N
  # example:
  # connection_name:
  #   speed: 100                                        # unit: MS/s
  #   endpoint: ${resource_name}===${resource_name}
  connections:
    io:
      speed: 500
      endpoint: ssda===cpu
    pcie0:
      speed: 11000
      endpoint: cpu===gpu0
*/
//    bool okay = true;
//    server::ConfigNode resource_config = GetConfig(CONFIG_RESOURCE);
//    if (resource_config.GetChildren().empty()) {
//        std::cerr << "ERROR: no context under resource" << std::endl;
//        okay = false;
//    }
//
//    auto resources = resource_config.GetChild(CONFIG_RESOURCES).GetChildren();
//
//    if (resources.empty()) {
//        std::cerr << "no resources specified" << std::endl;
//        okay = false;
//    }
//
//    bool resource_valid_flag = false;
//    bool hasDisk = false;
//    bool hasCPU = false;
//    bool hasExecutor = false;
//    std::set<std::string> resource_list;
//    for (auto &resource : resources) {
//        resource_list.emplace(resource.first);
//        auto &resource_conf = resource.second;
//        auto type = resource_conf.GetValue(CONFIG_RESOURCE_TYPE);
//
//        std::string device_id_str = resource_conf.GetValue(CONFIG_RESOURCE_DEVICE_ID, "0");
//        int32_t device_id = -1;
//        if (!ValidationUtil::ValidateStringIsNumber(device_id_str).ok()) {
//            std::cerr << "ERROR: device_id " << device_id_str << " is not a number" << std::endl;
//            okay = false;
//        } else {
//            device_id = std::stol(device_id_str);
//        }
//
//        std::string enable_executor_str = resource_conf.GetValue(CONFIG_RESOURCE_ENABLE_EXECUTOR, "off");
//        if (!ValidationUtil::ValidateStringIsBool(enable_executor_str).ok()) {
//            std::cerr << "ERROR: invalid enable_executor config: " << enable_executor_str << std::endl;
//            okay = false;
//        }
//
//        if (type == "DISK") {
//            hasDisk = true;
//        } else if (type == "CPU") {
//            hasCPU = true;
//            if (resource_conf.GetBoolValue(CONFIG_RESOURCE_ENABLE_EXECUTOR, false)) {
//                hasExecutor = true;
//            }
//        }
//        else if (type == "GPU") {
//            int build_index_gpu_index = GetConfig(CONFIG_DB).GetInt32Value(CONFIG_DB_BUILD_INDEX_GPU, 0);
//            if (device_id == build_index_gpu_index) {
//                resource_valid_flag = true;
//            }
//            if (resource_conf.GetBoolValue(CONFIG_RESOURCE_ENABLE_EXECUTOR, false)) {
//                hasExecutor = true;
//            }
//            std::string gpu_resource_num_str = resource_conf.GetValue(CONFIG_RESOURCE_NUM, "2");
//            if (!ValidationUtil::ValidateStringIsNumber(gpu_resource_num_str).ok()) {
//                std::cerr << "ERROR: gpu_resource_num " << gpu_resource_num_str << " is not a number" << std::endl;
//                okay = false;
//            }
//            bool mem_valid = true;
//            std::string pinned_memory_str = resource_conf.GetValue(CONFIG_RESOURCE_PIN_MEMORY, "300");
//            if (!ValidationUtil::ValidateStringIsNumber(pinned_memory_str).ok()) {
//                std::cerr << "ERROR: pinned_memory " << pinned_memory_str << " is not a number" << std::endl;
//                okay = false;
//                mem_valid = false;
//            }
//            std::string temp_memory_str = resource_conf.GetValue(CONFIG_RESOURCE_TEMP_MEMORY, "300");
//            if (!ValidationUtil::ValidateStringIsNumber(temp_memory_str).ok()) {
//                std::cerr << "ERROR: temp_memory " << temp_memory_str << " is not a number" << std::endl;
//                okay = false;
//                mem_valid = false;
//            }
//            if (mem_valid) {
//                size_t gpu_memory;
//                if (!ValidationUtil::GetGpuMemory(device_id, gpu_memory).ok()) {
//                    std::cerr << "ERROR: could not get gpu memory for device " << device_id << std::endl;
//                    okay = false;
//                }
//                else {
//                    size_t prealoc_mem = std::stol(pinned_memory_str) + std::stol(temp_memory_str);
//                    if (prealoc_mem >= gpu_memory) {
//                        std::cerr << "ERROR: sum of pinned_memory and temp_memory " << prealoc_mem
//                                  << " exceeds total gpu memory " << gpu_memory << " for device " << device_id << std::endl;
//                        okay = false;
//                    }
//                }
//            }
//        }
//    }
//
//    if (!resource_valid_flag) {
//        std::cerr << "Building index GPU can't be found in resource config." << std::endl;
//        okay = false;
//    }
//    if (!hasDisk || !hasCPU) {
//        std::cerr << "No DISK or CPU resource" << std::endl;
//        okay = false;
//    }
//    if (!hasExecutor) {
//        std::cerr << "No CPU or GPU resource has executor enabled" << std::endl;
//        okay = false;
//    }
//
//    auto connections = resource_config.GetChild(CONFIG_RESOURCE_CONNECTIONS).GetChildren();
//    for (auto &connection : connections) {
//        auto &connection_conf = connection.second;
//
//        std::string speed_str = connection_conf.GetValue(CONFIG_SPEED_CONNECTIONS);
//        if (ValidationUtil::ValidateStringIsNumber(speed_str) != SERVER_SUCCESS) {
//            std::cerr << "ERROR: speed " << speed_str << " is not a number" << std::endl;
//            okay = false;
//        }
//
//        std::string endpoint_str = connection_conf.GetValue(CONFIG_ENDPOINT_CONNECTIONS);
//        std::string delimiter = "===";
//        auto delimiter_pos = endpoint_str.find(delimiter);
//        if (delimiter_pos == std::string::npos) {
//            std::cerr << "ERROR: invalid endpoint format: " << endpoint_str << std::endl;
//            okay = false;
//        } else {
//            std::string left_resource = endpoint_str.substr(0, delimiter_pos);
//            if (resource_list.find(left_resource) == resource_list.end()) {
//                std::cerr << "ERROR: left resource " << left_resource << " does not exist" << std::endl;
//                okay = false;
//            }
//            std::string right_resource = endpoint_str.substr(delimiter_pos + delimiter.length(), endpoint_str.length());
//            if (resource_list.find(right_resource) == resource_list.end()) {
//                std::cerr << "ERROR: right resource " << right_resource << " does not exist" << std::endl;
//                okay = false;
//            }
//        }
//    }
//
//    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Resource config is illegal"));
//}

void
Config::PrintAll() const {
    if (const ConfigMgr *mgr = ConfigMgr::GetInstance()) {
        std::string str = mgr->DumpString();
//        SERVER_LOG_INFO << "\n" << str;
        std::cout << "\n" << str << std::endl;
    }
}

ConfigNode
Config::GetConfig(const std::string &name) const {
    const ConfigMgr *mgr = ConfigMgr::GetInstance();
    const ConfigNode &root_node = mgr->GetRootNode();
    return root_node.GetChild(name);
}

ConfigNode &
Config::GetConfig(const std::string &name) {
    ConfigMgr *mgr = ConfigMgr::GetInstance();
    ConfigNode &root_node = mgr->GetRootNode();
    return root_node.GetChild(name);
}

/* server config */
std::string
Config::GetServerConfigAddress() {
    ConfigNode server_config = GetConfig(CONFIG_SERVER);
    return server_config.GetValue(CONFIG_SERVER_ADDRESS,
                                  CONFIG_SERVER_ADDRESS_DEFAULT);
}

std::string
Config::GetServerConfigPort() {
    ConfigNode server_config = GetConfig(CONFIG_SERVER);
    return server_config.GetValue(CONFIG_SERVER_PORT,
                                  CONFIG_SERVER_PORT_DEFAULT);
}

std::string
Config::GetServerConfigMode() {
    ConfigNode server_config = GetConfig(CONFIG_SERVER);
    return server_config.GetValue(CONFIG_SERVER_MODE,
                                  CONFIG_SERVER_MODE_DEFAULT);
}

std::string
Config::GetServerConfigTimeZone() {
    ConfigNode server_config = GetConfig(CONFIG_SERVER);
    return server_config.GetValue(CONFIG_SERVER_TIME_ZONE,
                                  CONFIG_SERVER_TIME_ZONE_DEFAULT);
}

/* db config */
std::string
Config::GetDBConfigPath() {
    ConfigNode db_config = GetConfig(CONFIG_DB);
    return db_config.GetValue(CONFIG_DB_PATH,
                              CONFIG_DB_PATH_DEFAULT);
}

std::string
Config::GetDBConfigSlavePath() {
    ConfigNode db_config = GetConfig(CONFIG_DB);
    return db_config.GetValue(CONFIG_DB_SLAVE_PATH,
                              CONFIG_DB_SLAVE_PATH_DEFAULT);
}

std::string
Config::GetDBConfigBackendUrl() {
    ConfigNode db_config = GetConfig(CONFIG_DB);
    return db_config.GetValue(CONFIG_DB_BACKEND_URL,
                              CONFIG_DB_BACKEND_URL_DEFAULT);
}

int32_t
Config::GetDBConfigArchiveDiskThreshold() {
    ConfigNode db_config = GetConfig(CONFIG_DB);
    return db_config.GetInt32Value(CONFIG_DB_ARCHIVE_DISK_THRESHOLD,
                                   std::stoi(CONFIG_DB_ARCHIVE_DISK_THRESHOLD_DEFAULT));
}

int32_t
Config::GetDBConfigArchiveDaysThreshold() {
    ConfigNode db_config = GetConfig(CONFIG_DB);
    return db_config.GetInt32Value(CONFIG_DB_ARCHIVE_DAYS_THRESHOLD,
                                   std::stoi(CONFIG_DB_ARCHIVE_DAYS_THRESHOLD_DEFAULT));
}

int32_t
Config::GetDBConfigBufferSize() {
    ConfigNode db_config = GetConfig(CONFIG_DB);
    return db_config.GetInt32Value(CONFIG_DB_BUFFER_SIZE,
                                   std::stoi(CONFIG_DB_BUFFER_SIZE_DEFAULT));
}

int32_t
Config::GetDBConfigBuildIndexGPU() {
    ConfigNode db_config = GetConfig(CONFIG_DB);
    return db_config.GetInt32Value(CONFIG_DB_BUILD_INDEX_GPU,
                                   std::stoi(CONFIG_DB_BUILD_INDEX_GPU_DEFAULT));
}

/* metric config */
bool
Config::GetMetricConfigAutoBootup() {
    ConfigNode metric_config = GetConfig(CONFIG_METRIC);
    return metric_config.GetBoolValue(CONFIG_METRIC_AUTO_BOOTUP,
                                      std::stoi(CONFIG_METRIC_AUTO_BOOTUP_DEFAULT));
}

std::string
Config::GetMetricConfigCollector() {
    ConfigNode metric_config = GetConfig(CONFIG_METRIC);
    return metric_config.GetValue(CONFIG_METRIC_COLLECTOR,
                                  CONFIG_METRIC_COLLECTOR_DEFAULT);
}

std::string
Config::GetMetricConfigPrometheusPort() {
    ConfigNode metric_config = GetConfig(CONFIG_METRIC);
    return metric_config.GetValue(CONFIG_METRIC_PROMETHEUS_PORT,
                                  CONFIG_METRIC_PROMETHEUS_PORT_DEFAULT);
}

/* cache config */
int32_t
Config::GetCacheConfigCpuMemCapacity() {
    ConfigNode cache_config = GetConfig(CONFIG_CACHE);
    return cache_config.GetInt32Value(CONFIG_CACHE_CPU_MEM_CAPACITY,
                                      std::stoi(CONFIG_CACHE_CPU_MEM_CAPACITY_DEFAULT));
}

float
Config::GetCacheConfigCpuMemThreshold() {
    ConfigNode cache_config = GetConfig(CONFIG_CACHE);
    return cache_config.GetFloatValue(CONFIG_CACHE_CPU_MEM_THRESHOLD,
                                      std::stof(CONFIG_CACHE_CPU_MEM_THRESHOLD_DEFAULT));
}

int32_t
Config::GetCacheConfigGpuMemCapacity() {
    ConfigNode cache_config = GetConfig(CONFIG_CACHE);
    return cache_config.GetInt32Value(CONFIG_CACHE_GPU_MEM_CAPACITY,
                                      std::stoi(CONFIG_CACHE_GPU_MEM_CAPACITY_DEFAULT));
}

float
Config::GetCacheConfigGpuMemThreshold() {
    ConfigNode cache_config = GetConfig(CONFIG_CACHE);
    return cache_config.GetFloatValue(CONFIG_CACHE_GPU_MEM_THRESHOLD,
                                      std::stof(CONFIG_CACHE_GPU_MEM_THRESHOLD_DEFAULT));
}

bool
Config::GetCacheConfigCacheInsertData() {
    ConfigNode cache_config = GetConfig(CONFIG_CACHE);
    return cache_config.GetBoolValue(CONFIG_CACHE_CACHE_INSERT_DATA,
                                     std::stoi(CONFIG_CACHE_CACHE_INSERT_DATA_DEFAULT));
}

/* engine config */
int32_t
Config::GetEngineConfigBlasThreshold() {
    ConfigNode engine_config = GetConfig(CONFIG_ENGINE);
    return engine_config.GetInt32Value(CONFIG_ENGINE_BLAS_THRESHOLD,
                                       std::stoi(CONFIG_ENGINE_BLAS_THRESHOLD_DEFAULT));
}

int32_t
Config::GetEngineConfigOmpThreadNum() {
    ConfigNode engine_config = GetConfig(CONFIG_ENGINE);
    return engine_config.GetInt32Value(CONFIG_ENGINE_OMP_THREAD_NUM,
                                       std::stoi(CONFIG_ENGINE_OMP_THREAD_NUM_DEFAULT));
}

/* resource config */
std::string
Config::GetResourceConfigMode() {
    ConfigNode resource_config = GetConfig(CONFIG_RESOURCE);
    return resource_config.GetValue(CONFIG_RESOURCE_MODE,
                                    CONFIG_RESOURCE_MODE_DEFAULT);
}

std::vector<std::string>
Config::GetResourceConfigPool() {
    ConfigNode resource_config = GetConfig(CONFIG_RESOURCE);
    return resource_config.GetSequence(CONFIG_RESOURCE_POOL);
}

}
}
}
