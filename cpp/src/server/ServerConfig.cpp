////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "ServerConfig.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>

#include "config/IConfigMgr.h"

namespace zilliz {
namespace vecwise {
namespace server {

ServerConfig&
ServerConfig::GetInstance() {
    static ServerConfig config;
    return config;
}

ServerError
ServerConfig::LoadConfigFile(const std::string& config_filename) {
    std::string filename = config_filename;
    if(filename.empty()){
        std::cout << "ERROR: a config file is required" << std::endl;
        exit(1);//directly exit program if config file not specified
    }
    struct stat directoryStat;
    int statOK = stat(filename.c_str(), &directoryStat);
    if (statOK != 0) {
        std::cout << "ERROR: " << filename << " not found!" << std::endl;
        exit(1);//directly exit program if config file not found
    }

    try {
        IConfigMgr* mgr = const_cast<IConfigMgr*>(IConfigMgr::GetInstance());
        ServerError err = mgr->LoadConfigFile(filename);
        if(err != 0) {
            std::cout << "Server failed to load config file" << std::endl;
            exit(1);//directly exit program if the config file is illegal
        }
    }
    catch (YAML::Exception& e) {
        std::cout << "Server failed to load config file: " << std::endl;
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

void
ServerConfig::PrintAll() const {
    if(const IConfigMgr* mgr = IConfigMgr::GetInstance()) {
        std::string str = mgr->DumpString();
//        SERVER_LOG_INFO << "\n" << str;
        std::cout << "\n" << str << std::endl;
    }
}

ConfigNode
ServerConfig::GetConfig(const std::string& name) const {
    const IConfigMgr* mgr = IConfigMgr::GetInstance();
    const ConfigNode& root_node = mgr->GetRootNode();
    return root_node.GetChild(name);
}

ConfigNode&
ServerConfig::GetConfig(const std::string& name) {
    IConfigMgr* mgr = IConfigMgr::GetInstance();
    ConfigNode& root_node = mgr->GetRootNode();
    return root_node.GetChild(name);
}


}
}
}
