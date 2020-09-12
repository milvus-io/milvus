// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "config/YamlConfigMgr.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

Status
YamlConfigMgr::LoadConfigFile(const std::string& filename) {
    try {
        node_ = YAML::LoadFile(filename);
        LoadConfigNode(node_, config_);
    } catch (YAML::Exception& e) {
        std::string str = "Exception: load config file fail: " + std::string(e.what());
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }

    return Status::OK();
}

void
YamlConfigMgr::Print() const {
    LOG_SERVER_INFO_ << "System config content:";
    config_.PrintAll();
}

std::string
YamlConfigMgr::DumpString() const {
    return config_.DumpString("");
}

const ConfigNode&
YamlConfigMgr::GetRootNode() const {
    return config_;
}

ConfigNode&
YamlConfigMgr::GetRootNode() {
    return config_;
}

bool
YamlConfigMgr::SetConfigValue(const YAML::Node& node, const std::string& key, ConfigNode& config) {
    if (node[key].IsDefined()) {
        config.SetValue(key, node[key].as<std::string>());
        return true;
    }
    return false;
}

bool
YamlConfigMgr::SetChildConfig(const YAML::Node& node, const std::string& child_name, ConfigNode& config) {
    if (node[child_name].IsDefined()) {
        ConfigNode sub_config;
        LoadConfigNode(node[child_name], sub_config);
        config.AddChild(child_name, sub_config);
        return true;
    }
    return false;
}

bool
YamlConfigMgr::SetSequence(const YAML::Node& node, const std::string& child_name, ConfigNode& config) {
    if (node[child_name].IsDefined()) {
        size_t cnt = node[child_name].size();
        for (size_t i = 0; i < cnt; ++i) {
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
        if (!it->first.IsNull()) {
            key = it->first.as<std::string>();
        }
        if (node[key].IsScalar()) {
            SetConfigValue(node, key, config);
        } else if (node[key].IsMap()) {
            SetChildConfig(node, key, config);
        } else if (node[key].IsSequence()) {
            SetSequence(node, key, config);
        }
    }
}

}  // namespace server
}  // namespace milvus
