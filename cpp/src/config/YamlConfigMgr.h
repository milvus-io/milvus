/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IConfigMgr.h"
#include "ConfigNode.h"
#include "utils/Error.h"

#include <yaml-cpp/yaml.h>

namespace zilliz {
namespace milvus {
namespace server {

class YamlConfigMgr : public IConfigMgr {
 public:
    virtual ServerError LoadConfigFile(const std::string &filename);
    virtual void Print() const;
    virtual std::string DumpString() const;

    virtual const ConfigNode& GetRootNode() const;
    virtual ConfigNode& GetRootNode();

 private:
    bool SetConfigValue(const YAML::Node& node,
                        const std::string& key,
                        ConfigNode& config);

    bool SetChildConfig(const YAML::Node& node,
                        const std::string &name,
                        ConfigNode &config);

    bool
    SetSequence(const YAML::Node &node,
                const std::string &child_name,
                ConfigNode &config);

    void LoadConfigNode(const YAML::Node& node, ConfigNode& config);

 private:
    YAML::Node node_;
    ConfigNode config_;
};

}
}
}


