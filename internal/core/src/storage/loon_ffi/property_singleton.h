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

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

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
            ApplyArrowReaderConfig(*properties_);
        }
    }

    void
    Init(const char* root_path) {
        std::unique_lock lck(mutex_);

        if (properties_ == nullptr) {
            properties_ = MakeInternalLocalProperies(root_path);
            ApplyArrowReaderConfig(*properties_);
        }
    }

    void
    SetArrowReaderConfig(int64_t hole_size_limit_bytes,
                         int64_t range_size_limit_bytes) {
        std::unique_lock lck(mutex_);
        arrow_reader_hole_size_limit_bytes_ = hole_size_limit_bytes;
        arrow_reader_range_size_limit_bytes_ = range_size_limit_bytes;
        if (properties_ != nullptr) {
            auto properties =
                std::make_shared<milvus_storage::api::Properties>(*properties_);
            ApplyArrowReaderConfig(*properties);
            properties_ = std::move(properties);
        }
    }

    std::shared_ptr<milvus_storage::api::Properties>
    GetProperties() const {
        std::shared_lock lck(mutex_);
        return properties_;
    }

 private:
    void
    ApplyArrowReaderConfig(milvus_storage::api::Properties& properties) const {
        milvus_storage::api::SetValue(
            properties,
            PROPERTY_READER_PARQUET_PREBUFFER_HOLE_SIZE_LIMIT,
            std::to_string(arrow_reader_hole_size_limit_bytes_).c_str());
        milvus_storage::api::SetValue(
            properties,
            PROPERTY_READER_PARQUET_PREBUFFER_RANGE_SIZE_LIMIT,
            std::to_string(arrow_reader_range_size_limit_bytes_).c_str());
    }

    mutable std::shared_mutex mutex_;
    std::shared_ptr<milvus_storage::api::Properties> properties_ = nullptr;
    int64_t arrow_reader_hole_size_limit_bytes_ = 0;
    int64_t arrow_reader_range_size_limit_bytes_ = 0;
};

}  // namespace milvus::storage
