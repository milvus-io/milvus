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

#include <string>

#include "config/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

class ConfigHandler {
 public:
    ConfigHandler() = default;
    virtual ~ConfigHandler() = default;

 protected:
    void
    SetIdentity(const std::string& identity) {
        auto& config = server::Config::GetInstance();
        config.GenUniqueIdentityID(identity, identity_);
    }

 protected:
    std::string identity_;
};

}  // namespace server
}  // namespace milvus
