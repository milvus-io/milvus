/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include <string>
#include <map>

namespace zilliz {
namespace vecwise {
namespace server {

class ConfigNode;
typedef std::vector<ConfigNode> ConfigNodeArr;

class ConfigNode {
 public:
    void Combine(const ConfigNode& target);

    //key/value pair config
    void SetValue(const std::string &key, const std::string &value);

    std::string GetValue(const std::string &param_key, const std::string &default_val = "") const;
    bool GetBoolValue(const std::string &param_key, bool default_val = false) const;
    int32_t GetInt32Value(const std::string &param_key, int32_t default_val = 0) const;
    int64_t GetInt64Value(const std::string &param_key, int64_t default_val = 0) const;
    float GetFloatValue(const std::string &param_key, float default_val = 0.0) const;
    double GetDoubleValue(const std::string &param_key, double default_val = 0.0) const;

    const std::map<std::string, std::string>& GetConfig() const;
    void ClearConfig();

    //key/object config
    void AddChild(const std::string &type_name, const ConfigNode &config);
    ConfigNode GetChild(const std::string &type_name) const;
    ConfigNode& GetChild(const std::string &type_name);
    void GetChildren(ConfigNodeArr &arr) const;

    const std::map<std::string, ConfigNode>& GetChildren() const;
    void ClearChildren();

    //key/sequence config
    void AddSequenceItem(const std::string &key, const std::string &item);
    std::vector<std::string> GetSequence(const std::string &key) const;

    const std::map<std::string, std::vector<std::string> >& GetSequences() const;
    void ClearSequences();

    void PrintAll(const std::string &prefix = "") const;
    std::string DumpString(const std::string &prefix = "") const;

 private:
    std::map<std::string, std::string> config_;
    std::map<std::string, ConfigNode> children_;
    std::map<std::string, std::vector<std::string> > sequences_;
};

}
}
}
