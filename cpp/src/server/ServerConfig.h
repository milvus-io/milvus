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

#pragma once

#include "utils/Status.h"
#include "config/ConfigNode.h"

#include "yaml-cpp/yaml.h"

namespace zilliz {
namespace milvus {
namespace server {

/* server config */
static const char* CONFIG_SERVER = "server_config";
static const char* CONFIG_SERVER_ADDRESS = "address";
static const char* CONFIG_SERVER_ADDRESS_DEFAULT = "127.0.0.1";
static const char* CONFIG_SERVER_PORT = "port";
static const char* CONFIG_SERVER_PORT_DEFAULT = "19530";
static const char* CONFIG_SERVER_MODE = "mode";
static const char* CONFIG_SERVER_MODE_DEFAULT = "single";
static const char* CONFIG_SERVER_TIME_ZONE = "time_zone";
static const char* CONFIG_SERVER_TIME_ZONE_DEFAULT = "UTC+8";

/* db config */
static const char* CONFIG_DB = "db_config";
static const char* CONFIG_DB_PATH = "db_path";
static const char* CONFIG_DB_PATH_DEFAULT = "/tmp/milvus";
static const char* CONFIG_DB_SLAVE_PATH = "db_slave_path";
static const char* CONFIG_DB_SLAVE_PATH_DEFAULT = "";
static const char* CONFIG_DB_BACKEND_URL = "db_backend_url";
static const char* CONFIG_DB_BACKEND_URL_DEFAULT = "sqlite://:@:/";
static const char* CONFIG_DB_ARCHIVE_DISK_THRESHOLD = "archive_disk_threshold";
static const char* CONFIG_DB_ARCHIVE_DISK_THRESHOLD_DEFAULT = "0";
static const char* CONFIG_DB_ARCHIVE_DAYS_THRESHOLD = "archive_days_threshold";
static const char* CONFIG_DB_ARCHIVE_DAYS_THRESHOLD_DEFAULT = "0";
static const char* CONFIG_DB_BUFFER_SIZE = "buffer_size";
static const char* CONFIG_DB_BUFFER_SIZE_DEFAULT = "4";
static const char* CONFIG_DB_BUILD_INDEX_GPU = "build_index_gpu";
static const char* CONFIG_DB_BUILD_INDEX_GPU_DEFAULT = "0";

/* cache config */
static const char* CONFIG_CACHE = "cache_config";
static const char* CONFIG_CACHE_CPU_MEM_CAPACITY = "cpu_mem_capacity";
static const char* CONFIG_CACHE_CPU_MEM_CAPACITY_DEFAULT = "16";
static const char* CONFIG_CACHE_GPU_MEM_CAPACITY = "gpu_mem_capacity";
static const char* CONFIG_CACHE_GPU_MEM_CAPACITY_DEFAULT = "0";
static const char* CONFIG_CACHE_CPU_MEM_THRESHOLD = "cpu_mem_threshold";
static const char* CONFIG_CACHE_CPU_MEM_THRESHOLD_DEFAULT = "0.85";
static const char* CONFIG_CACHE_GPU_MEM_THRESHOLD = "gpu_mem_threshold";
static const char* CONFIG_CACHE_GPU_MEM_THRESHOLD_DEFAULT = "0.85";
static const char* CONFIG_CACHE_INSERT_IMMEDIATELY = "insert_immediately";
static const char* CONFIG_CACHE_INSERT_IMMEDIATELY_DEFAULT = "0";

/* metric config */
static const char* CONFIG_METRIC = "metric_config";
static const char* CONFIG_METRIC_AUTO_BOOTUP = "auto_bootup";
static const char* CONFIG_METRIC_AUTO_BOOTUP_DEFAULT = "0";
static const char* CONFIG_METRIC_COLLECTOR = "collector";
static const char* CONFIG_METRIC_COLLECTOR_DEFAULT = "prometheus";
static const char* CONFIG_METRIC_PROMETHEUS = "prometheus_config";
static const char* CONFIG_METRIC_PROMETHEUS_PORT = "port";
static const char* CONFIG_METRIC_PROMETHEUS_PORT_DEFAULT = "8080";

/* engine config */
static const char* CONFIG_ENGINE = "engine_config";
static const char* CONFIG_ENGINE_BLAS_THRESHOLD = "blas_threshold";
static const char* CONFIG_ENGINE_BLAS_THRESHOLD_DEFAULT = "20";
static const char* CONFIG_ENGINE_OMP_THREAD_NUM = "omp_thread_num";
static const char* CONFIG_ENGINE_OMP_THREAD_NUM_DEFAULT = "0";

/* resource config */
static const char* CONFIG_RESOURCE = "resource_config";
static const char* CONFIG_RESOURCE_MODE = "mode";
static const char* CONFIG_RESOURCE_MODE_DEFAULT = "simple";
static const char* CONFIG_RESOURCE_POOL = "pool";


class ServerConfig {
 public:
    static ServerConfig &GetInstance();

    Status LoadConfigFile(const std::string& config_filename);
    Status ValidateConfig();
    void PrintAll() const;

    ConfigNode GetConfig(const std::string& name) const;
    ConfigNode& GetConfig(const std::string& name);

 private:
    Status CheckServerConfig();
    Status CheckDBConfig();
    Status CheckMetricConfig();
    Status CheckCacheConfig();
    Status CheckEngineConfig();
    Status CheckResourceConfig();
};

}
}
}

