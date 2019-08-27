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

#include "config/ConfigMgr.h"
#include "utils/CommonUtil.h"
#include "utils/ValidationUtil.h"

namespace zilliz {
namespace milvus {
namespace server {

constexpr uint64_t MB = 1024*1024;
constexpr uint64_t GB = MB*1024;

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
        ConfigMgr* mgr = const_cast<ConfigMgr*>(ConfigMgr::GetInstance());
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

ServerError ServerConfig::ValidateConfig() const {
    //server config validation
    ConfigNode server_config = GetConfig(CONFIG_SERVER);
    uint32_t gpu_index = (uint32_t)server_config.GetInt32Value(CONFIG_GPU_INDEX, 0);
    if(ValidationUtil::ValidateGpuIndex(gpu_index) != SERVER_SUCCESS) {
        std::cout << "Error: invalid gpu_index " << std::to_string(gpu_index) << std::endl;
        return SERVER_INVALID_ARGUMENT;
    }

    //db config validation
    unsigned long total_mem = 0, free_mem = 0;
    CommonUtil::GetSystemMemInfo(total_mem, free_mem);

    ConfigNode db_config = GetConfig(CONFIG_DB);
    uint64_t insert_buffer_size = (uint64_t)db_config.GetInt32Value(CONFIG_DB_INSERT_BUFFER_SIZE, 4);
    insert_buffer_size *= GB;
    if(insert_buffer_size >= total_mem) {
        std::cout << "Error: insert_buffer_size execeed system memory" << std::endl;
        return SERVER_INVALID_ARGUMENT;
    }

    //cache config validation
    ConfigNode cache_config = GetConfig(CONFIG_CACHE);
    uint64_t cache_cap = (uint64_t)cache_config.GetInt64Value(CONFIG_CPU_CACHE_CAPACITY, 16);
    cache_cap *= GB;
    if(cache_cap >= total_mem) {
        std::cout << "Error: cpu_cache_capacity execeed system memory" << std::endl;
        return SERVER_INVALID_ARGUMENT;
    } if(cache_cap > (double)total_mem*0.9) {
        std::cout << "Warnning: cpu_cache_capacity value is too aggressive" << std::endl;
    }

    if(insert_buffer_size + cache_cap >= total_mem) {
        std::cout << "Error: sum of cpu_cache_capacity and insert_buffer_size execeed system memory" << std::endl;
        return SERVER_INVALID_ARGUMENT;
    }

    double free_percent = cache_config.GetDoubleValue(server::CACHE_FREE_PERCENT, 0.85);
    if(free_percent < std::numeric_limits<double>::epsilon() || free_percent > 1.0) {
        std::cout << "Error: invalid cache_free_percent " << std::to_string(free_percent) << std::endl;
        return SERVER_INVALID_ARGUMENT;
    }

    return SERVER_SUCCESS;
}

void
ServerConfig::PrintAll() const {
    if(const ConfigMgr* mgr = ConfigMgr::GetInstance()) {
        std::string str = mgr->DumpString();
//        SERVER_LOG_INFO << "\n" << str;
        std::cout << "\n" << str << std::endl;
    }
}

ConfigNode
ServerConfig::GetConfig(const std::string& name) const {
    const ConfigMgr* mgr = ConfigMgr::GetInstance();
    const ConfigNode& root_node = mgr->GetRootNode();
    return root_node.GetChild(name);
}

ConfigNode&
ServerConfig::GetConfig(const std::string& name) {
    ConfigMgr* mgr = ConfigMgr::GetInstance();
    ConfigNode& root_node = mgr->GetRootNode();
    return root_node.GetChild(name);
}


}
}
}
