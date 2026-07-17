// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "storage/plugin/PluginInterface.h"

namespace milvus::test {

class PlannerCipherPlugin : public storage::plugin::ICipherPlugin {
 public:
    std::string
    getPluginName() const override {
        return "CipherPlugin";
    }

    void
    Update(int64_t, int64_t, const std::string&) override {
    }

    std::pair<std::shared_ptr<storage::plugin::IEncryptor>, std::string>
    GetEncryptor(int64_t, int64_t) const override {
        return {nullptr, {}};
    }

    std::shared_ptr<storage::plugin::IDecryptor>
    GetDecryptor(int64_t, int64_t, const std::string&) const override {
        return nullptr;
    }
};

}  // namespace milvus::test
