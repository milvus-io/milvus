/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientApp.h"
#include "server/ServerConfig.h"
#include "Log.h"


namespace zilliz {
namespace vecwise {
namespace client {


void ClientApp::Run(const std::string &config_file) {
    server::ServerConfig& config = server::ServerConfig::GetInstance();
    config.LoadConfigFile(config_file);

    CLIENT_LOG_INFO << "Load config file:" << config_file;
}

}
}
}

