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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

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
    GetEncryptor(int64_t ez_id, int64_t coll_id) const = 0;

    virtual std::shared_ptr<IDecryptor>
    GetDecryptor(int64_t ez_id,
                 int64_t coll_id,
                 const std::string& safeKey) const = 0;
};

class IEncryptor {
 public:
    virtual ~IEncryptor() = default;

    virtual std::string
    Encrypt(const std::string& plaintext) const = 0;

    virtual std::string
    Encrypt(std::string_view plaintext) const = 0;

    virtual std::string
    Encrypt(const void* data, size_t len) const = 0;

    virtual std::string
    GetKey() const = 0;
};

class IDecryptor {
 public:
    virtual ~IDecryptor() = default;

    virtual std::string
    Decrypt(const std::string& ciphertext) const = 0;

    virtual std::string
    Decrypt(std::string_view ciphertext) const = 0;

    virtual std::string
    Decrypt(const void* data, size_t len) const = 0;

    virtual std::string
    GetKey() const = 0;
};

}  // namespace plugin
}  // namespace milvus::storage
