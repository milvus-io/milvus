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

#pragma once

#include <yaml-cpp/yaml.h>
#include <string>

#include "ConfigMgr.h"
#include "utils/Status.h"

namespace milvus {
namespace server {

class YamlConfigMgr : public ConfigMgr {
 public:
    static ConfigMgr*
    GetInstance() {
        static YamlConfigMgr mgr;
        return &mgr;
    }

    virtual Status
    LoadConfigFile(const std::string& filename);

    virtual void
    Print() const;

    virtual std::string
    DumpString() const;

    virtual const ConfigNode&
    GetRootNode() const;

    virtual ConfigNode&
    GetRootNode();

 private:
    bool
    SetConfigValue(const YAML::Node& node, const std::string& key, ConfigNode& config);

    bool
    SetChildConfig(const YAML::Node& node, const std::string& child_name, ConfigNode& config);

    bool
    SetSequence(const YAML::Node& node, const std::string& child_name, ConfigNode& config);

    void
    LoadConfigNode(const YAML::Node& node, ConfigNode& config);

 private:
    YAML::Node node_;
    ConfigNode config_;
};

}  // namespace server
}  // namespace milvus
