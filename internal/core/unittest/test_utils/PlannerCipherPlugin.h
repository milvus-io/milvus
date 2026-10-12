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
#include <stdexcept>
#include <string>
#include <string_view>
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

class PlannerIdentityEncryptor : public storage::plugin::IEncryptor {
 public:
    std::string
    Encrypt(const std::string& plaintext) const override {
        return plaintext;
    }

    std::string
    Encrypt(std::string_view plaintext) const override {
        return std::string(plaintext);
    }

    std::string
    Encrypt(const void* data, size_t len) const override {
        return std::string(static_cast<const char*>(data), len);
    }

    std::string
    GetKey() const override {
        return {};
    }
};

class PlannerIdentityDecryptor : public storage::plugin::IDecryptor {
 public:
    std::string
    Decrypt(const std::string& ciphertext) const override {
        return ciphertext;
    }

    std::string
    Decrypt(std::string_view ciphertext) const override {
        return std::string(ciphertext);
    }

    std::string
    Decrypt(const void* data, size_t len) const override {
        return std::string(static_cast<const char*>(data), len);
    }

    std::string
    GetKey() const override {
        return {};
    }
};

class CollectionBoundPlannerCipherPlugin
    : public storage::plugin::ICipherPlugin {
 public:
    explicit CollectionBoundPlannerCipherPlugin(int64_t collection_id)
        : collection_id_(collection_id) {
    }

    std::string
    getPluginName() const override {
        return "CipherPlugin";
    }

    void
    Update(int64_t, int64_t, const std::string&) override {
    }

    std::pair<std::shared_ptr<storage::plugin::IEncryptor>, std::string>
    GetEncryptor(int64_t, int64_t collection_id) const override {
        CheckCollection(collection_id);
        return {std::make_shared<PlannerIdentityEncryptor>(), "planner_edek"};
    }

    std::shared_ptr<storage::plugin::IDecryptor>
    GetDecryptor(int64_t,
                 int64_t collection_id,
                 const std::string&) const override {
        CheckCollection(collection_id);
        return std::make_shared<PlannerIdentityDecryptor>();
    }

 private:
    void
    CheckCollection(int64_t collection_id) const {
        if (collection_id != collection_id_) {
            throw std::runtime_error("unexpected collection id");
        }
    }

    int64_t collection_id_;
};

}  // namespace milvus::test
