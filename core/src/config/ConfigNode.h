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

#include <map>
#include <string>
#include <vector>

namespace milvus {
namespace server {

class ConfigNode;
typedef std::vector<ConfigNode> ConfigNodeArr;

class ConfigNode {
 public:
    void
    Combine(const ConfigNode& target);

    // key/value pair config
    void
    SetValue(const std::string& key, const std::string& value);

    std::string
    GetValue(const std::string& param_key, const std::string& default_val = "") const;
    bool
    GetBoolValue(const std::string& param_key, bool default_val = false) const;
    int32_t
    GetInt32Value(const std::string& param_key, int32_t default_val = 0) const;
    int64_t
    GetInt64Value(const std::string& param_key, int64_t default_val = 0) const;
    float
    GetFloatValue(const std::string& param_key, float default_val = 0.0) const;
    double
    GetDoubleValue(const std::string& param_key, double default_val = 0.0) const;

    const std::map<std::string, std::string>&
    GetConfig() const;
    void
    ClearConfig();

    // key/object config
    void
    AddChild(const std::string& type_name, const ConfigNode& config);
    ConfigNode
    GetChild(const std::string& type_name) const;
    ConfigNode&
    GetChild(const std::string& type_name);
    void
    GetChildren(ConfigNodeArr& arr) const;

    const std::map<std::string, ConfigNode>&
    GetChildren() const;
    void
    ClearChildren();

    // key/sequence config
    void
    AddSequenceItem(const std::string& key, const std::string& item);
    std::vector<std::string>
    GetSequence(const std::string& key) const;

    const std::map<std::string, std::vector<std::string> >&
    GetSequences() const;
    void
    ClearSequences();

    void
    PrintAll(const std::string& prefix = "") const;
    std::string
    DumpString(const std::string& prefix = "") const;

 private:
    std::map<std::string, std::string> config_;
    std::map<std::string, ConfigNode> children_;
    std::map<std::string, std::vector<std::string> > sequences_;
};

}  // namespace server
}  // namespace milvus
