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

#include "config/ConfigNode.h"
#include "utils/Error.h"
#include "utils/Log.h"

#include <fiu-local.h>
#include <algorithm>
#include <sstream>
#include <string>

namespace milvus {
namespace server {

void
ConfigNode::Combine(const ConfigNode& target) {
    const std::map<std::string, std::string>& kv = target.GetConfig();
    for (auto itr = kv.begin(); itr != kv.end(); ++itr) {
        config_[itr->first] = itr->second;
    }

    const std::map<std::string, std::vector<std::string> >& sequences = target.GetSequences();
    for (auto itr = sequences.begin(); itr != sequences.end(); ++itr) {
        sequences_[itr->first] = itr->second;
    }

    const std::map<std::string, ConfigNode>& children = target.GetChildren();
    for (auto itr = children.begin(); itr != children.end(); ++itr) {
        children_[itr->first] = itr->second;
    }
}

// key/value pair config
void
ConfigNode::SetValue(const std::string& key, const std::string& value) {
    config_[key] = value;
}

std::string
ConfigNode::GetValue(const std::string& param_key, const std::string& default_val) const {
    auto ref = config_.find(param_key);
    if (ref != config_.end()) {
        return ref->second;
    }

    // THROW_UNEXPECTED_ERROR("Can't find parameter key: " + param_key);
    return default_val;
}

bool
ConfigNode::GetBoolValue(const std::string& param_key, bool default_val) const {
    std::string val = GetValue(param_key);
    if (!val.empty()) {
        std::transform(val.begin(), val.end(), val.begin(), ::tolower);
        return (val == "true" || val == "on" || val == "yes" || val == "1");
    } else {
        return default_val;
    }
}

int32_t
ConfigNode::GetInt32Value(const std::string& param_key, int32_t default_val) const {
    std::string val = GetValue(param_key);
    if (!val.empty()) {
        return (int32_t)std::strtol(val.c_str(), nullptr, 10);
    } else {
        return default_val;
    }
}

int64_t
ConfigNode::GetInt64Value(const std::string& param_key, int64_t default_val) const {
    std::string val = GetValue(param_key);
    if (!val.empty()) {
        return std::strtol(val.c_str(), nullptr, 10);
    } else {
        return default_val;
    }
}

float
ConfigNode::GetFloatValue(const std::string& param_key, float default_val) const {
    std::string val = GetValue(param_key);
    if (!val.empty()) {
        return std::strtof(val.c_str(), nullptr);
    } else {
        return default_val;
    }
}

double
ConfigNode::GetDoubleValue(const std::string& param_key, double default_val) const {
    std::string val = GetValue(param_key);
    if (!val.empty()) {
        return std::strtod(val.c_str(), nullptr);
    } else {
        return default_val;
    }
}

const std::map<std::string, std::string>&
ConfigNode::GetConfig() const {
    return config_;
}

void
ConfigNode::ClearConfig() {
    config_.clear();
}

// key/object config
void
ConfigNode::AddChild(const std::string& type_name, const ConfigNode& config) {
    children_[type_name] = config;
}

ConfigNode
ConfigNode::GetChild(const std::string& type_name) const {
    auto ref = children_.find(type_name);
    if (ref != children_.end()) {
        return ref->second;
    }

    ConfigNode nc;
    return nc;
}

ConfigNode&
ConfigNode::GetChild(const std::string& type_name) {
    return children_[type_name];
}

void
ConfigNode::GetChildren(ConfigNodeArr& arr) const {
    arr.clear();
    arr.reserve(children_.size());
    transform(children_.begin(), children_.end(), back_inserter(arr), [](auto& ref) { return ref.second; });
}

const std::map<std::string, ConfigNode>&
ConfigNode::GetChildren() const {
    return children_;
}

void
ConfigNode::ClearChildren() {
    children_.clear();
}

// key/sequence config
void
ConfigNode::AddSequenceItem(const std::string& key, const std::string& item) {
    sequences_[key].push_back(item);
}

std::vector<std::string>
ConfigNode::GetSequence(const std::string& key) const {
    auto itr = sequences_.find(key);
    if (itr != sequences_.end()) {
        return itr->second;
    } else {
        std::vector<std::string> temp;
        return temp;
    }
}

const std::map<std::string, std::vector<std::string> >&
ConfigNode::GetSequences() const {
    return sequences_;
}

void
ConfigNode::ClearSequences() {
    sequences_.clear();
}

void
ConfigNode::PrintAll(const std::string& prefix) const {
    for (auto& elem : config_) {
        LOG_SERVER_INFO_ << prefix << elem.first + ": " << elem.second;
    }

    for (auto& elem : sequences_) {
        LOG_SERVER_INFO_ << prefix << elem.first << ": ";
        for (auto& str : elem.second) {
            LOG_SERVER_INFO_ << prefix << "    - " << str;
        }
    }

    for (auto& elem : children_) {
        LOG_SERVER_INFO_ << prefix << elem.first << ": ";
        elem.second.PrintAll(prefix + "    ");
    }
}

std::string
ConfigNode::DumpString(const std::string& prefix) const {
    std::stringstream str_buffer;
    const std::string endl = "\n";
    for (auto& elem : config_) {
        str_buffer << prefix << elem.first << ": " << elem.second << endl;
    }

    for (auto& elem : sequences_) {
        str_buffer << prefix << elem.first << ": " << endl;
        for (auto& str : elem.second) {
            str_buffer << prefix + "    - " << str << endl;
        }
    }

    for (auto& elem : children_) {
        str_buffer << prefix << elem.first << ": " << endl;
        str_buffer << elem.second.DumpString(prefix + "    ") << endl;
    }

    return str_buffer.str();
}

}  // namespace server
}  // namespace milvus
