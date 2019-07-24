/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"
#include "config/ConfigNode.h"

#include "yaml-cpp/yaml.h"

namespace zilliz {
namespace milvus {
namespace server {

static const std::string CONFIG_SERVER = "server_config";
static const std::string CONFIG_SERVER_ADDRESS = "address";
static const std::string CONFIG_SERVER_PORT = "port";
static const std::string CONFIG_SERVER_PROTOCOL = "transfer_protocol";
static const std::string CONFIG_CLUSTER_MODE = "mode";
static const std::string CONFIG_GPU_INDEX = "gpu_index";

static const std::string CONFIG_DB = "db_config";
static const std::string CONFIG_DB_URL = "db_backend_url";
static const std::string CONFIG_DB_PATH = "db_path";
static const std::string CONFIG_DB_SLAVE_PATH = "db_slave_path";
static const std::string CONFIG_DB_INDEX_TRIGGER_SIZE = "index_building_threshold";
static const std::string CONFIG_DB_ARCHIVE_DISK = "archive_disk_threshold";
static const std::string CONFIG_DB_ARCHIVE_DAYS = "archive_days_threshold";
static const std::string CONFIG_DB_INSERT_BUFFER_SIZE = "insert_buffer_size";
static const std::string CONFIG_DB_PARALLEL_REDUCE = "parallel_reduce";

static const std::string CONFIG_LOG = "log_config";

static const std::string CONFIG_CACHE = "cache_config";
static const std::string CONFIG_CPU_CACHE_CAPACITY = "cpu_cache_capacity";
static const std::string CONFIG_GPU_CACHE_CAPACITY = "gpu_cache_capacity";
static const std::string CACHE_FREE_PERCENT = "cache_free_percent";
static const std::string CONFIG_INSERT_CACHE_IMMEDIATELY = "insert_cache_immediately";

static const std::string CONFIG_LICENSE = "license_config";
static const std::string CONFIG_LICENSE_PATH = "license_path";

static const std::string CONFIG_METRIC = "metric_config";
static const std::string CONFIG_METRIC_IS_STARTUP = "is_startup";
static const std::string CONFIG_METRIC_COLLECTOR = "collector";
static const std::string CONFIG_PROMETHEUS = "prometheus_config";
static const std::string CONFIG_METRIC_PROMETHEUS_PORT = "port";

static const std::string CONFIG_ENGINE = "engine_config";
static const std::string CONFIG_NPROBE = "nprobe";
static const std::string CONFIG_NLIST = "nlist";
static const std::string CONFIG_DCBT = "use_blas_threshold";
static const std::string CONFIG_METRICTYPE = "metric_type";

class ServerConfig {
 public:
    static ServerConfig &GetInstance();

    ServerError LoadConfigFile(const std::string& config_filename);
    ServerError ValidateConfig() const;
    void PrintAll() const;

    ConfigNode GetConfig(const std::string& name) const;
    ConfigNode& GetConfig(const std::string& name);
};

}
}
}

