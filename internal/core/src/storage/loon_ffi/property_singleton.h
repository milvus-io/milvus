// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>

#include "common/type_c.h"
#include "milvus-storage/properties.h"
#include "util.h"

namespace milvus::storage {

class LoonFFIPropertiesSingleton {
 private:
    LoonFFIPropertiesSingleton() = default;

 public:
    static LoonFFIPropertiesSingleton&
    GetInstance() {
        static LoonFFIPropertiesSingleton instance;
        return instance;
    }

    void
    Init(CStorageConfig c_storage_config) {
        std::unique_lock lck(mutex_);

        if (properties_ == nullptr) {
            properties_ =
                MakeInternalPropertiesFromStorageConfig(c_storage_config);
        }
    }

    void
    Init(const char* root_path) {
        std::unique_lock lck(mutex_);

        if (properties_ == nullptr) {
            properties_ = MakeInternalLocalProperies(root_path);
        }
    }

    std::shared_ptr<milvus_storage::api::Properties>
    GetProperties() const {
        std::shared_lock lck(mutex_);
        return properties_;
    }

 private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<milvus_storage::api::Properties> properties_ = nullptr;
};

}  // namespace milvus::storage