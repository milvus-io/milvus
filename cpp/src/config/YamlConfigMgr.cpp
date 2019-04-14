/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "YamlConfigMgr.h"
#include "utils/CommonUtil.h"

#include <sys/stat.h>

namespace zilliz {
namespace vecwise {
namespace server {

ServerError YamlConfigMgr::LoadConfigFile(const std::string &filename) {
    struct stat directoryStat;
    int statOK = stat(filename.c_str(), &directoryStat);
    if (statOK != 0) {
        CommonUtil::PrintError("File not found: " + filename);
        return SERVER_UNEXPECTED_ERROR;
    }

    try {
        node_ = YAML::LoadFile(filename);
        LoadConfigNode(node_, config_);
    }
    catch (YAML::Exception& e) {
        CommonUtil::PrintError("Failed to load config file: " + std::string(e.what ()));
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

void YamlConfigMgr::Print() const {
    CommonUtil::PrintInfo("System config content:");
    config_.PrintAll();
}

std::string YamlConfigMgr::DumpString() const {
    return config_.DumpString("");
}

const ConfigNode& YamlConfigMgr::GetRootNode() const {
    return config_;
}

ConfigNode& YamlConfigMgr::GetRootNode() {
    return config_;
}

bool
YamlConfigMgr::SetConfigValue(const YAML::Node& node,
                             const std::string& key,
                             ConfigNode& config) {
    if(node[key].IsDefined ()) {
        config.SetValue(key, node[key].as<std::string>());
        return true;
    }
    return false;
}

bool
YamlConfigMgr::SetChildConfig(const YAML::Node& node,
                             const std::string& child_name,
                             ConfigNode& config) {
    if(node[child_name].IsDefined ()) {
        ConfigNode sub_config;
        LoadConfigNode(node[child_name], sub_config);
        config.AddChild(child_name, sub_config);
        return true;
    }
    return false;
}

bool
YamlConfigMgr::SetSequence(const YAML::Node &node,
                           const std::string &child_name,
                           ConfigNode &config) {
    if(node[child_name].IsDefined ()) {
        size_t cnt = node[child_name].size();
        for(size_t i = 0; i < cnt; i++){
            config.AddSequenceItem(child_name, node[child_name][i].as<std::string>());
        }
        return true;
    }
    return false;
}

void
YamlConfigMgr::LoadConfigNode(const YAML::Node& node, ConfigNode& config) {
    std::string key;
    for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
        if(!it->first.IsNull()){
            key = it->first.as<std::string>();
        }
        if(node[key].IsScalar()) {
            SetConfigValue(node, key, config);
        } else if(node[key].IsMap()){
            SetChildConfig(node, key, config);
        } else if(node[key].IsSequence()){
            SetSequence(node, key, config);
        }
    }
}

}
}
}
