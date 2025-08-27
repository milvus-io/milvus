// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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
#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <string>

namespace milvus::storage {
namespace plugin {

class IEncryptor;
class IDecryptor;
class ICipherPlugin;

class IPlugin {
 public:
    virtual ~IPlugin() = default;

    virtual std::string
    getPluginName() const = 0;
};

class ICipherPlugin : public IPlugin {
 public:
    virtual ~ICipherPlugin() = default;

    std::string
    getPluginName() const override {
        return "ICipherPlugin";
    }

    virtual void
    Update(int64_t ez_id, int64_t coll_id, const std::string& key) = 0;

    virtual std::pair<std::shared_ptr<IEncryptor>, std::string>
    GetEncryptor(int64_t ez_id, int64_t coll_id) = 0;

    virtual std::shared_ptr<IDecryptor>
    GetDecryptor(int64_t ez_id,
                 int64_t coll_id,
                 const std::string& safeKey) = 0;
};

class IEncryptor {
 public:
    virtual ~IEncryptor() = default;
    virtual std::string
    Encrypt(const std::string& plaintext) = 0;

    virtual std::string
    GetKey() = 0;
};

class IDecryptor {
 public:
    virtual ~IDecryptor() = default;
    virtual std::string
    Decrypt(const std::string& ciphertext) = 0;

    virtual std::string
    GetKey() = 0;
};

}  // namespace plugin
}  // namespace milvus::storage
