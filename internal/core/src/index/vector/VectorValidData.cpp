// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/vector/VectorValidData.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "nlohmann/json.hpp"

#include "common/EasyAssert.h"
#include "common/GrowingOffsetMapping.h"
#include "common/SealedOffsetMapping.h"
#include "index/Meta.h"
#include "index/vector/VectorIndexValidDataUtils.h"

namespace milvus::index {

namespace {

constexpr int64_t kMaxOffsetMappingCount =
    static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;

std::shared_ptr<const milvus::OffsetMapping>
NewNoOpMapping() {
    return std::make_shared<milvus::NoOpOffsetMapping>();
}

bool
GetMmapFlag(const Config& config, const char* key) {
    if (!config.contains(key) || config.at(key).is_null()) {
        return false;
    }

    const auto& encoded = config.at(key);
    if (encoded.is_boolean()) {
        return encoded.get<bool>();
    }
    if (encoded.is_string()) {
        const auto& value = encoded.get_ref<const std::string&>();
        const bool is_true = value.size() == 4 &&
                             (value[0] == 't' || value[0] == 'T') &&
                             (value[1] == 'r' || value[1] == 'R') &&
                             (value[2] == 'u' || value[2] == 'U') &&
                             (value[3] == 'e' || value[3] == 'E');
        const bool is_false = value.size() == 5 &&
                              (value[0] == 'f' || value[0] == 'F') &&
                              (value[1] == 'a' || value[1] == 'A') &&
                              (value[2] == 'l' || value[2] == 'L') &&
                              (value[3] == 's' || value[3] == 'S') &&
                              (value[4] == 'e' || value[4] == 'E');
        if (is_true || is_false) {
            return is_true;
        }
    }
    ThrowInfo(
        DataTypeInvalid, "nullable vector parameter {} must be boolean", key);
}

}  // namespace

// Owns only one generated child. The caller's parent directory is borrowed and
// is never removed.
class VectorValidDataDirectory final {
 public:
    static std::shared_ptr<VectorValidDataDirectory>
    Create(const std::string& parent) {
        if (parent.empty()) {
            ThrowInfo(ConfigInvalid,
                      "nullable vector offset mapping mmap parent is empty");
        }

        std::error_code error;
        std::filesystem::create_directories(parent, error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create nullable vector offset mapping root "
                      "{}: {}",
                      parent,
                      error.message());
        }

        auto pattern =
            (std::filesystem::path(parent) / "valid_data_XXXXXX").string();
        // Acquire the lifetime owner before mkdtemp. Once the directory is
        // created, arming cleanup cannot allocate or throw.
        auto result = std::shared_ptr<VectorValidDataDirectory>(
            new VectorValidDataDirectory(std::move(pattern)));
        if (::mkdtemp(result->path_.data()) == nullptr) {
            ThrowInfo(FileCreateFailed,
                      "failed to create nullable vector offset mapping "
                      "directory in {}: {}",
                      parent,
                      std::strerror(errno));
        }
        result->created_ = true;
        return result;
    }

    ~VectorValidDataDirectory() {
        if (created_) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    VectorValidDataDirectory(const VectorValidDataDirectory&) = delete;
    VectorValidDataDirectory&
    operator=(const VectorValidDataDirectory&) = delete;

    const std::string&
    Path() const {
        return path_;
    }

 private:
    explicit VectorValidDataDirectory(std::string path)
        : path_(std::move(path)) {
    }

    std::string path_;
    bool created_{false};
};

VectorValidData::VectorValidData() : mapping_(NewNoOpMapping()) {
}

VectorValidData::VectorValidData(
    std::shared_ptr<const milvus::OffsetMapping> mapping)
    : mapping_(std::move(mapping)) {
    AssertInfo(mapping_ != nullptr,
               "growing vector validity snapshot is null");
}

VectorValidData::~VectorValidData() = default;

VectorValidData
VectorValidData::FromGrowingSnapshot(
    const milvus::GrowingOffsetMapping& mapping) {
    return VectorValidData(mapping.Snapshot());
}

VectorValidData&
VectorValidData::operator=(const VectorValidData& other) noexcept {
    if (this == &other) {
        return *this;
    }
    auto mapping = other.mapping_;
    auto directory = other.directory_;
    mapping_ = std::move(mapping);
    directory_ = std::move(directory);
    return *this;
}

VectorValidData&
VectorValidData::operator=(VectorValidData&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    mapping_ = std::move(other.mapping_);
    directory_ = std::move(other.directory_);
    return *this;
}

OffsetMappingBuildOptions
GetOffsetMappingMmapOptions(const Config& config) {
    OffsetMappingBuildOptions options;
    options.enable_mmap_i2o_map = GetMmapFlag(config, ENABLE_MMAP_I2O_MAP);
    options.enable_mmap_o2i_map = GetMmapFlag(config, ENABLE_MMAP_O2I_MAP);
    return options;
}

void
VectorValidData::Build(const bool* valid_data,
                       int64_t total_count,
                       const milvus::OffsetMappingBuildOptions& options) {
    if (total_count < 0 || total_count > kMaxOffsetMappingCount) {
        ThrowInfo(DataTypeInvalid,
                  "nullable vector row count {} is outside the supported "
                  "offset-mapping domain [0, {}]",
                  total_count,
                  kMaxOffsetMappingCount);
    }
    if (total_count > 0 && valid_data == nullptr) {
        ThrowInfo(DataTypeInvalid,
                  "nullable vector validity data is null for {} rows",
                  total_count);
    }

    if (total_count == 0) {
        auto mapping = NewNoOpMapping();
        mapping_ = std::move(mapping);
        directory_.reset();
        return;
    }

    bool need_mmap = options.enable_mmap_o2i_map;
    if (!need_mmap && options.enable_mmap_i2o_map) {
        for (int64_t i = 0; i < total_count; ++i) {
            if (valid_data[i]) {
                need_mmap = true;
                break;
            }
        }
    }

    std::shared_ptr<VectorValidDataDirectory> directory;
    auto build_options = options;
    if (need_mmap) {
        directory = VectorValidDataDirectory::Create(options.mmap_dir_path);
        build_options.mmap_dir_path = directory->Path();
    }

    auto mapping = std::make_shared<milvus::SealedOffsetMapping>();
    mapping->Build(valid_data, total_count, build_options);

    // Publish only after every allocation/file/mmap operation has succeeded.
    mapping_ = std::move(mapping);
    directory_ = std::move(directory);
}

void
VectorValidData::Build(ValidityView valid_data,
                       int64_t total_count,
                       const milvus::OffsetMappingBuildOptions& options) {
    if (const auto* expanded = valid_data.expanded_data();
        expanded != nullptr) {
        Build(expanded, total_count, options);
        return;
    }
    if (total_count <= 0) {
        Build(nullptr, total_count, options);
        return;
    }
    AssertInfo(valid_data,
               "nullable vector validity view is empty for {} rows",
               total_count);
    auto expanded = std::make_unique<bool[]>(static_cast<size_t>(total_count));
    for (int64_t row = 0; row < total_count; ++row) {
        expanded[static_cast<size_t>(row)] = valid_data[row];
    }
    Build(expanded.get(), total_count, options);
}

}  // namespace milvus::index
