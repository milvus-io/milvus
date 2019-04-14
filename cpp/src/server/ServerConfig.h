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
static const std::string CONFIG_ADDRESS = "address";
static const std::string CONFIG_PORT = "port";


class ServerConfig {
 public:
    static ServerConfig *GetInstance();

    ServerError LoadConfigFile(const std::string& config_filename);
    void PrintAll() const;

    ConfigNode GetServerConfig() const;
    ConfigNode& GetServerConfig();

    std::string GetServerAddress() const;
    std::string GetServerPort() const;
};

}
}
}

