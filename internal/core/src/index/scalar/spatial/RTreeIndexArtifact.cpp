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

#include "index/scalar/spatial/RTreeIndexArtifact.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <limits>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {
namespace {

constexpr std::string_view kArchiveEntry = "index_file.bgi";
constexpr std::string_view kMetadataEntry = "index_file.meta.json";
constexpr std::string_view kArchiveSuffix = ".bgi";

bool
EndsWith(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
           value.substr(value.size() - suffix.size()) == suffix;
}

void
ValidateEngineFiles(const std::vector<std::string>& files) {
    std::set<std::string> unique;
    size_t archives = 0;
    for (const auto& name : files) {
        const std::filesystem::path path(name);
        if (name.empty() || name.find('\0') != std::string::npos ||
            path.filename().string() != name || name == "." || name == ".." ||
            name == INDEX_NULL_OFFSET || !unique.insert(name).second) {
            ThrowInfo(DataFormatBroken,
                      "invalid or duplicate R-Tree engine entry {}",
                      name);
        }
        archives += EndsWith(name, kArchiveSuffix) ? 1 : 0;
    }
    if (archives != 1) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree artifact must contain exactly one .bgi archive, got "
                  "{}",
                  archives);
    }
}

std::vector<std::string>
DiscoverBuilderFiles(const std::string& directory) {
    std::vector<std::string> files;
    for (const auto name : {kArchiveEntry, kMetadataEntry}) {
        const auto path =
            (std::filesystem::path(directory) / std::string(name)).string();
        std::error_code error;
        const auto exists = std::filesystem::is_regular_file(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect R-Tree artifact file {}: {}",
                      path,
                      error.message());
        }
        if (exists) {
            files.emplace_back(name);
        }
    }
    ValidateEngineFiles(files);
    std::sort(files.begin(), files.end());
    return files;
}

}  // namespace

std::shared_ptr<storage::LocalDirectory>
CreateRTreeIndexDirectory(const std::string& parent, std::string_view label) {
    AssertInfo(parent.find('\0') == std::string::npos,
               "R-Tree staging parent contains an embedded NUL");
    std::error_code error;
    auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                               : std::filesystem::path(parent);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to locate temporary directory for R-Tree: {}",
                  error.message());
    }
    std::string safe_label;
    safe_label.reserve(std::min<size_t>(label.size(), 24));
    for (const auto value : label) {
        if (safe_label.size() == 24) {
            break;
        }
        const auto byte = static_cast<unsigned char>(value);
        safe_label.push_back(
            std::isalnum(byte) || value == '-' || value == '_' ? value : '_');
    }
    const auto pattern = safe_label.empty()
                             ? std::string("milvus-rtree-XXXXXX")
                             : "milvus-rtree-" + safe_label + "-XXXXXX";
    return storage::LocalDirectory::CreateOwned(
        root.string(), pattern.c_str(), "R-Tree");
}

RTreeIndexArtifact::RTreeIndexArtifact(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::vector<size_t> null_offsets) {
    AssertInfo(directory != nullptr,
               "R-Tree artifact requires an owned directory");
    auto engine_files = DiscoverBuilderFiles(directory->Path());
    state_ = ArtifactState{std::move(directory),
                           std::move(engine_files),
                           std::move(null_offsets)};
}

RTreeIndexArtifact::~RTreeIndexArtifact() = default;

void
RTreeIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto& directory = state_.directory;
    const auto& files = state_.engine_files;
    const auto& null_offsets = state_.null_offsets;
    AssertInfo(directory != nullptr, "R-Tree artifact has no directory owner");
    ValidateEngineFiles(files);
    std::vector<std::pair<std::string, std::string>> local_files;
    local_files.reserve(files.size());
    for (const auto& name : files) {
        const auto path =
            (std::filesystem::path(directory->Path()) / name).string();
        std::error_code error;
        const auto exists = std::filesystem::is_regular_file(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect R-Tree artifact file {}: {}",
                      path,
                      error.message());
        }
        if (!exists) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree artifact is missing engine entry {}",
                      name);
        }
        local_files.emplace_back(name, std::move(path));
    }

    if (sink.Gen() == storage::Generation::V3) {
        std::vector<std::string> file_names;
        file_names.reserve(local_files.size());
        for (const auto& [name, _] : local_files) {
            file_names.push_back(name);
        }
        sink.PutMeta(FILE_NAMES, file_names);
        sink.PutMeta(HAS_NULL, !null_offsets.empty());
    }
    for (const auto& [name, path] : local_files) {
        sink.WriteEntryFromLocalFile(name, path);
    }
    if (!null_offsets.empty()) {
        AssertInfo(null_offsets.size() <=
                       std::numeric_limits<size_t>::max() / sizeof(size_t),
                   "R-Tree null-offset byte size overflows size_t");
        sink.WriteEntry(INDEX_NULL_OFFSET,
                        null_offsets.data(),
                        null_offsets.size() * sizeof(size_t));
    }
}

}  // namespace milvus::index
