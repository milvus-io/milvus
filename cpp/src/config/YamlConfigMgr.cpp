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

#include "YamlConfigMgr.h"
#include "utils/Log.h"

#include <sys/stat.h>

namespace zilliz {
namespace milvus {
namespace server {

ErrorCode YamlConfigMgr::LoadConfigFile(const std::string &filename) {
    struct stat directoryStat;
    int statOK = stat(filename.c_str(), &directoryStat);
    if (statOK != 0) {
        SERVER_LOG_ERROR << "File not found: " << filename;
        return SERVER_UNEXPECTED_ERROR;
    }

    try {
        node_ = YAML::LoadFile(filename);
        LoadConfigNode(node_, config_);
    }
    catch (YAML::Exception& e) {
        SERVER_LOG_ERROR << "Failed to load config file: " << std::string(e.what ());
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

void YamlConfigMgr::Print() const {
    SERVER_LOG_INFO << "System config content:";
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
