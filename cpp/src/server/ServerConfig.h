/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"
#include "config/ConfigNode.h"

#include <yaml-cpp/yaml.h>

namespace zilliz {
namespace vecwise {
namespace server {

static const std::string CONFIG_SERVER = "server_config";
static const std::string CONFIG_SERVER_ADDRESS = "address";
static const std::string CONFIG_SERVER_PORT = "port";
static const std::string CONFIG_SERVER_PROTOCOL = "transfer_protocol";
static const std::string CONFIG_SERVER_MODE = "server_mode";

static const std::string CONFIG_DB = "db_config";
static const std::string CONFIG_DB_URL = "db_backend_url";
static const std::string CONFIG_DB_PATH = "db_path";
static const std::string CONFIG_DB_FLUSH_INTERVAL = "db_flush_interval";
static const std::string CONFIG_DB_IDMAPPER_MAX_FILE = "idmapper_max_open_file";

static const std::string CONFIG_LOG = "log_config";

static const std::string CONFIG_CACHE = "cache_config";
static const std::string CONFIG_CPU_CACHE_CAPACITY = "cpu_cache_capacity";
static const std::string CONFIG_GPU_CACHE_CAPACITY = "gpu_cache_capacity";

static const std::string CONFIG_LICENSE = "license_config";
static const std::string CONFIG_LICENSE_PATH = "license_path";

static const std::string CONFIG_METRIC = "metric_config";
static const std::string CONFIG_METRIC_IS_STARTUP = "is_startup";
static const std::string CONFIG_METRIC_COLLECTOR = "collector";

class ServerConfig {
 public:
    static ServerConfig &GetInstance();

    ServerError LoadConfigFile(const std::string& config_filename);
    void PrintAll() const;

    ConfigNode GetConfig(const std::string& name) const;
    ConfigNode& GetConfig(const std::string& name);
};

}
}
}

